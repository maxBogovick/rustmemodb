use rustmemodb::InMemoryDB;
use rustmemodb::connection::config::ConnectionConfig;
use rustmemodb::connection::pool::ConnectionPool;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

pub struct LoadTestConfig {
    pub duration_secs: u64,
    pub concurrency: usize,
    pub rows: usize,
    pub read_ratio: u8,
    pub sample_max: usize,
    pub payload_size: usize,
}

pub async fn run_load_test(config: LoadTestConfig) -> Result<(), Box<dyn Error>> {
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    let pool_config = ConnectionConfig::new("admin", "adminpass")
        .max_connections(config.concurrency.max(1))
        .min_connections(config.concurrency.min(2).max(1));
    let pool = Arc::new(ConnectionPool::new_with_db(pool_config, db).await?);

    let mut setup_conn = pool.get_connection().await?;
    setup_conn
        .execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, payload TEXT)")
        .await?;
    let payload = "x".repeat(config.payload_size);
    let mut next_id = 0usize;
    let batch_size = 500usize;

    while next_id < config.rows {
        let end = (next_id + batch_size).min(config.rows);
        let mut values = String::new();
        for id in next_id..end {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str(&format!("({}, '{}')", id, payload));
        }
        let sql = format!("INSERT INTO bench VALUES {}", values);
        setup_conn.execute(&sql).await?;
        next_id = end;
    }
    setup_conn.close().await?;

    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let per_task_sample = (config.sample_max / config.concurrency.max(1)).max(1);

    #[cfg(feature = "pprof")]
    let pprof_guard = maybe_start_pprof();

    let start = std::time::Instant::now();
    let deadline = start + std::time::Duration::from_secs(config.duration_secs);
    let mut handles = Vec::with_capacity(config.concurrency);

    for worker_id in 0..config.concurrency {
        let pool = pool.clone();
        let read_count = read_count.clone();
        let write_count = write_count.clone();
        let error_count = error_count.clone();
        let deadline = deadline;
        let rows = config.rows as u64;
        let read_ratio = config.read_ratio;
        let payload = payload.clone();
        let mut rng = Lcg64::new(0x9e3779b97f4a7c15 ^ worker_id as u64);
        let handle = tokio::spawn(async move {
            let mut conn = match pool.get_connection().await {
                Ok(conn) => conn,
                Err(_) => {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    return Vec::new();
                }
            };
            let mut latencies = Vec::with_capacity(per_task_sample);
            while std::time::Instant::now() < deadline {
                let roll = (rng.next_u64() % 100) as u8;
                let id = rng.next_u64() % rows.max(1);
                let sql = if roll < read_ratio {
                    format!("SELECT * FROM bench WHERE id = {}", id)
                } else {
                    format!("UPDATE bench SET payload = '{}' WHERE id = {}", payload, id)
                };
                let op_start = std::time::Instant::now();
                match conn.execute(&sql).await {
                    Ok(_) => {
                        if roll < read_ratio {
                            read_count.fetch_add(1, Ordering::Relaxed);
                        } else {
                            write_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if latencies.len() < per_task_sample {
                    latencies.push(op_start.elapsed().as_micros() as u64);
                }
            }
            let _ = conn.close().await;
            latencies
        });
        handles.push(handle);
    }

    let mut latencies = Vec::new();
    for handle in handles {
        let mut worker_latencies = handle.await?;
        latencies.append(&mut worker_latencies);
    }

    let total_ops = read_count.load(Ordering::Relaxed) + write_count.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    latencies.sort_unstable();

    let p50 = percentile(&latencies, 0.50);
    let p95 = percentile(&latencies, 0.95);
    let p99 = percentile(&latencies, 0.99);

    println!("load_test results:");
    println!("  duration_s: {:.2}", elapsed);
    println!("  total_ops: {}", total_ops);
    println!("  read_ops: {}", read_count.load(Ordering::Relaxed));
    println!("  write_ops: {}", write_count.load(Ordering::Relaxed));
    println!("  error_ops: {}", error_count.load(Ordering::Relaxed));
    println!("  qps: {:.2}", total_ops as f64 / elapsed);
    println!("  latency_us_p50: {}", p50);
    println!("  latency_us_p95: {}", p95);
    println!("  latency_us_p99: {}", p99);

    #[cfg(feature = "pprof")]
    maybe_write_pprof(pprof_guard)?;

    Ok(())
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

struct Lcg64 {
    state: u64,
}

impl Lcg64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }
}

#[cfg(feature = "pprof")]
fn maybe_start_pprof() -> Option<pprof::ProfilerGuard<'static>> {
    match std::env::var("RUSTMEMODB_PPROF").ok().as_deref() {
        Some("1") => pprof::ProfilerGuard::new(100).ok(),
        _ => None,
    }
}

#[cfg(feature = "pprof")]
fn maybe_write_pprof(guard: Option<pprof::ProfilerGuard<'static>>) -> Result<(), Box<dyn Error>> {
    if let Some(guard) = guard {
        let report = guard.report().build()?;
        let output =
            std::env::var("RUSTMEMODB_PPROF_OUTPUT").unwrap_or_else(|_| "pprof.svg".to_string());
        let file = std::fs::File::create(output)?;
        report.flamegraph(file)?;
    }
    Ok(())
}
