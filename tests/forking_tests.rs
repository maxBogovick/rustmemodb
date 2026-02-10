use rustmemodb::{Client, Result};

#[tokio::test]
async fn test_cow_forking_isolation() -> Result<()> {
    // 1. Setup Master DB (The "Seed")
    let master = Client::connect_local("admin", "adminpass").await?;

    // Seed data
    master
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .await?;
    master
        .execute("INSERT INTO users VALUES (1, 'Alice')")
        .await?;
    master
        .execute("INSERT INTO users VALUES (2, 'Bob')")
        .await?;

    let master_count = master.query("SELECT * FROM users").await?.row_count();
    assert_eq!(master_count, 2);

    // 2. Fork Test A (Modifies data)
    let test_a = master.fork().await?;
    test_a
        .execute("INSERT INTO users VALUES (3, 'Charlie')")
        .await?;
    test_a.execute("DELETE FROM users WHERE id = 1").await?;

    let count_a = test_a.query("SELECT * FROM users").await?.row_count();
    assert_eq!(count_a, 2); // 3 created, 1 deleted -> 2 left (Bob, Charlie)

    // Verify Charlie exists in A
    let result = test_a.query("SELECT name FROM users WHERE id = 3").await?;
    assert_eq!(
        result.rows()[0].get(0).unwrap().as_str().unwrap(),
        "Charlie"
    );

    // 3. Fork Test B (Parallel isolated test)
    let test_b = master.fork().await?;
    // Should see original seed state
    let count_b = test_b.query("SELECT * FROM users").await?.row_count();
    assert_eq!(count_b, 2); // Alice, Bob

    // Modify B
    test_b
        .execute("UPDATE users SET name = 'Bobby' WHERE id = 2")
        .await?;

    // 4. Verify Master is untouched
    let master_check = master.query("SELECT * FROM users ORDER BY id").await?;
    assert_eq!(master_check.row_count(), 2);
    assert_eq!(
        master_check.rows()[0].get(1).unwrap().as_str().unwrap(),
        "Alice"
    ); // Not deleted
    assert_eq!(
        master_check.rows()[1].get(1).unwrap().as_str().unwrap(),
        "Bob"
    ); // Not updated

    println!("COW Forking Test Passed: Full isolation verified!");
    Ok(())
}

#[tokio::test]
async fn test_fork_bench_simulation() -> Result<()> {
    let master = Client::connect_local("admin", "adminpass").await?;
    master
        .execute("CREATE TABLE huge_table (id INTEGER)")
        .await?;

    // Simulate seeding heavy data (50,000 rows)
    // In a deep-copy scenario, this would take significant time to clone.
    // In COW, it should be constant time (dominated by connection authentication overhead).
    for i in 0..50_000 {
        // Batching would be faster but we test simple inserts
        if i % 1000 == 0 {
            // Just to keep connection alive and not timeout if we had one
        }
        master
            .execute(&format!("INSERT INTO huge_table VALUES ({})", i))
            .await?;
    }

    // Measure fork time
    let start = std::time::Instant::now();
    let _fork = master.fork().await?;
    let duration = start.elapsed();

    println!("Forked database with 50,000 rows in {:?}", duration);

    // Allow up to 2 seconds (bcrypt overhead is ~500ms, plus 50k rows allocation if it failed)
    // If COW failed, 50k rows * allocations would likely exceed 2s in debug mode or be very noticeable.
    assert!(
        duration.as_millis() < 2000,
        "Forking took too long! COW might not be working."
    );

    Ok(())
}
