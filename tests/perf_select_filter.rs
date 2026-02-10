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

    let setup_start = Instant::now();
    db.execute("CREATE TABLE filter_perf (id INTEGER, val INTEGER)")
        .await
        .unwrap();
    let setup_ms = setup_start.elapsed().as_millis();

    let insert_start = Instant::now();
    for i in 0..row_count {
        let val = i % 1_000;
        db.execute(&format!("INSERT INTO filter_perf VALUES ({}, {})", i, val))
            .await
            .unwrap();
    }
    let insert_ms = insert_start.elapsed().as_millis();

    let start = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM filter_perf WHERE val = {}",
            target_value
        ))
        .await
        .unwrap();
    let duration = start.elapsed();
    let select_ms = duration.as_millis();

    assert_eq!(result.row_count(), 0);

    println!(
        "perf_select_filter_full_scan timings: setup={}ms insert={}ms select={}ms",
        setup_ms, insert_ms, select_ms
    );
    let cfg = perf_utils::start_run().unwrap();
    perf_utils::record_metric(&cfg, "select_filter_full_scan", duration).unwrap();
    perf_utils::record_metric(
        &cfg,
        "select_filter_full_scan_setup",
        std::time::Duration::from_millis(setup_ms as u64),
    )
    .unwrap();
    perf_utils::record_metric(
        &cfg,
        "select_filter_full_scan_insert",
        std::time::Duration::from_millis(insert_ms as u64),
    )
    .unwrap();
    perf_utils::finalize_run(&cfg).unwrap();
}
