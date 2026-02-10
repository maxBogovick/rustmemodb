use rustmemodb::InMemoryDB;
use std::time::Instant;

#[path = "perf_utils.rs"]
mod perf_utils;

#[tokio::test]
#[ignore]
async fn perf_wal_bulk_insert() {
    let mut db = InMemoryDB::new();
    db.enable_persistence("out/perf_wal", rustmemodb::storage::DurabilityMode::Sync)
        .await
        .unwrap();

    db.execute("CREATE TABLE IF NOT EXISTS wal_perf (id INTEGER, val INTEGER)")
        .await
        .unwrap();

    let row_count = 10_000;
    let start = Instant::now();
    for i in 0..row_count {
        db.execute(&format!("INSERT INTO wal_perf VALUES ({}, {})", i, i))
            .await
            .unwrap();
    }
    let duration = start.elapsed();

    let cfg = perf_utils::start_run().unwrap();
    perf_utils::record_metric(&cfg, "wal_bulk_insert", duration).unwrap();
    perf_utils::finalize_run(&cfg).unwrap();
}
