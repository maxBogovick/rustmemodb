use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_vacuum_reclaims_space() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE test (id INTEGER, val INTEGER)")
        .await
        .unwrap();

    // 1. Insert initial row
    db.execute("INSERT INTO test VALUES (1, 100)")
        .await
        .unwrap();

    // 2. Update it 5 times
    for i in 1..=5 {
        db.execute(&format!("UPDATE test SET val = {} WHERE id = 1", 100 + i))
            .await
            .unwrap();
    }

    // At this point, we have 1 (insert) + 5 (updates) = 6 versions.
    // 5 of them are "old" (overwritten).
    // Since we are using auto-commit (execute), all previous txs are committed.
    // There are no active transactions holding onto old versions.

    // 3. Run Vacuum
    let freed = db.vacuum().await.unwrap();

    // We expect 5 old versions to be freed.
    // Version chain:
    // V6 (Head, visible)
    // V5 (Dead, xmax=tx6)
    // V4 (Dead, xmax=tx5)
    // ...
    // V1 (Dead, xmax=tx2)

    println!("Vacuum freed {} versions", freed);
    assert_eq!(freed, 5);

    // 4. Verify data is still correct
    let result = db
        .execute("SELECT val FROM test WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(result.rows()[0][0], Value::Integer(105));
}

#[tokio::test]
async fn test_vacuum_respects_active_transactions() {
    use std::sync::Arc;

    let db = Arc::new(tokio::sync::RwLock::new(InMemoryDB::new()));

    // Setup
    {
        let mut db_write = db.write().await;
        db_write
            .execute("CREATE TABLE test (id INTEGER, val INTEGER)")
            .await
            .unwrap();
        db_write
            .execute("INSERT INTO test VALUES (1, 10)")
            .await
            .unwrap();
    } // Drop write lock

    // 1. Start a long-running transaction (Reader)
    let db_clone = db.clone();
    let reader_handle = tokio::spawn(async move {
        // Get transaction manager without holding long lock
        let tx_mgr = {
            let db_read = db_clone.read().await;
            db_read.transaction_manager().clone()
        };

        let tx_id = tx_mgr.begin().await.unwrap();

        // Sleep WITHOUT holding DB lock
        // This allows other updates to proceed while this transaction is effectively "open"
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Verify we still see old value
        // We need write lock to execute
        let mut db_write = db_clone.write().await;
        let result = db_write
            .execute_with_transaction("SELECT val FROM test WHERE id = 1", Some(tx_id))
            .await
            .unwrap();
        assert_eq!(result.rows()[0][0], Value::Integer(10));

        // Commit
        tx_mgr.commit(tx_id).await.unwrap();
    });

    // 2. Update the row in separate transactions (Writers)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    {
        let mut db_write = db.write().await;
        db_write
            .execute("UPDATE test SET val = 20 WHERE id = 1")
            .await
            .unwrap();
        db_write
            .execute("UPDATE test SET val = 30 WHERE id = 1")
            .await
            .unwrap();
    }

    // 3. Run Vacuum while Reader is still active
    {
        let db_read = db.read().await;
        let freed = db_read.vacuum().await.unwrap();
        // Since Reader started BEFORE the updates, it needs to see V1 (10).
        // V1 was updated by T_Update1. T_Update1 committed.
        // But V1 is needed by Reader.
        // V2 (20) was updated by T_Update2. T_Update2 committed.
        // V2 is NOT needed by Reader (Reader sees V1).
        // Is V2 visible to anyone?
        // Reader sees V1.
        // New txs see V3 (30).
        // So V2 might be garbage?
        // Wait, Reader snapshot has max_tx_id = T_start.
        // T_Update1 > T_start.
        // So Reader sees V1 (if V1.xmax > T_start or V1.xmax is active).
        // V1.xmax is T_Update1. T_Update1 is committed.
        // If T_Update1 > T_start, then for Reader, T_Update1 is "future".
        // Reader checks V1.xmax. If is_committed(xmax) returns true?
        // Snapshot logic: is_committed returns false if tx_id >= snapshot.max_tx_id.
        // So Reader thinks T_Update1 is NOT committed.
        // So Reader sees V1.

        // Therefore, V1 CANNOT be vacuumed.

        // What about V2?
        // V2 created by T_Update1.
        // V2 deleted by T_Update2.
        // Reader does not see V2 (created by future tx).
        // New txs see V3.
        // So V2 is not visible to Reader, nor to New txs (they see V3).
        // V2 IS garbage?
        // Vacuum check: V2.xmax = T_Update2.
        // min_active_tx_id = Reader.tx_id.
        // T_Update2 > Reader.tx_id.
        // Vacuum condition: xmax < min_active.
        // T_Update2 is NOT < Reader.tx_id.
        // So V2 is preserved (conservatively).

        // So Vacuum should free 0 versions.
        println!("Vacuum freed {} versions (expected 0)", freed);
        assert_eq!(freed, 0);
    }

    reader_handle.await.unwrap();

    // 4. Run Vacuum after Reader finishes
    {
        let db_read = db.read().await;
        let freed = db_read.vacuum().await.unwrap();
        // Now Reader is gone. min_active is new max.
        // V1 and V2 are dead.
        println!("Vacuum freed {} versions (expected 2)", freed);
        assert_eq!(freed, 2);
    }
}
