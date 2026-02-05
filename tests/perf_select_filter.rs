use rustmemodb::InMemoryDB;
use std::time::Instant;

#[path = "perf_utils.rs"]
mod perf_utils;

#[tokio::test]
#[ignore]
async fn perf_select_filter_full_scan() {
    let mut db = InMemoryDB::new();
    let row_count = 50_000;
    let target_value = 9_999;

    db.execute("CREATE TABLE filter_perf (id INTEGER, val INTEGER)").await.unwrap();
    for i in 0..row_count {
        let val = i % 1_000;
        db.execute(&format!("INSERT INTO filter_perf VALUES ({}, {})", i, val)).await.unwrap();
    }

    let start = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM filter_perf WHERE val = {}",
            target_value
        ))
        .await
        .unwrap();
    let duration = start.elapsed();

    assert_eq!(result.row_count(), 0);

    let cfg = perf_utils::start_run().unwrap();
    perf_utils::record_metric(&cfg, "select_filter_full_scan", duration).unwrap();
    perf_utils::finalize_run(&cfg).unwrap();
}
