/// Benchmark: RustMemoDB vs PostgreSQL
///
/// This example runs a head-to-head comparison between embedded RustMemoDB and a local PostgreSQL instance.
///
/// Prerequisites:
/// 1. A running PostgreSQL instance:
///    `docker run --rm --name pg-bench -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres`
///
/// Run with:
///    `cargo run --example benchmark_vs_postgres --release`

use rustmemodb::{InMemoryDB, Result};
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;
use rand::Rng;

const ROW_COUNT: usize = 100_000;
const SELECT_ITERATIONS: usize = 10_000;

#[tokio::main]
async fn main() -> Result<()> {
    println!("⚔️  RUSTMEMODB vs POSTGRESQL BENCHMARK ⚔️");
    println!("===========================================");
    println!("Configuration:");
    println!("  Rows: {}", ROW_COUNT);
    println!("  Select Iterations: {}", SELECT_ITERATIONS);
    println!("===========================================\n");

    // Generate Data
    println!("🎲 Generating {} random rows...", ROW_COUNT);
    let data = generate_data(ROW_COUNT);
    println!("✅ Data generated.\n");

    // ------------------------------------------------------------------------
    // RUSTMEMODB BENCHMARK
    // ------------------------------------------------------------------------
    println!("🦀 Benchmarking RustMemoDB...");
    let mut db = InMemoryDB::new();
    
    // 1. Bulk Insert
    let start = Instant::now();
    db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, info TEXT)").await?;
    db.execute("CREATE INDEX idx_val ON bench (val)").await?;
    
    // In a real scenario, we'd use prepared statements or bulk insert, 
    // but for now we iterate to test parser/executor throughput too.
    for row in &data {
        let sql = format!("INSERT INTO bench VALUES ({}, {}, '{}')", row.id, row.val, row.info);
        db.execute(&sql).await?;
    }
    let duration_mem_insert = start.elapsed();
    println!("  -> Insert: {:.2?}", duration_mem_insert);

    // 2. Point Select (by Index)
    let start = Instant::now();
    let mut hits = 0;
    let mut rng = rand::rng();
    for _ in 0..SELECT_ITERATIONS {
        let target_val = rng.random_range(0..ROW_COUNT as i64);
        let sql = format!("SELECT * FROM bench WHERE val = {}", target_val);
        let res = db.execute(&sql).await?;
        if res.row_count() > 0 { hits += 1; }
    }
    let duration_mem_select = start.elapsed();
    println!("  -> Select (Indexed): {:.2?}", duration_mem_select);

    // 3. Aggregation (Full Scan)
    let start = Instant::now();
    db.execute("SELECT AVG(val) FROM bench").await?;
    let duration_mem_aggr = start.elapsed();
    println!("  -> Aggregation (Full Scan): {:.2?}", duration_mem_aggr);


    // ------------------------------------------------------------------------
    // POSTGRES BENCHMARK
    // ------------------------------------------------------------------------
    println!("\n🐘 Benchmarking PostgreSQL...");
    match run_postgres_bench(&data).await {
        Ok((dur_pg_insert, dur_pg_select, dur_pg_aggr)) => {
            println!("  -> Insert: {:.2?}", dur_pg_insert);
            println!("  -> Select (Indexed): {:.2?}", dur_pg_select);
            println!("  -> Aggregation (Full Scan): {:.2?}", dur_pg_aggr);

            // ----------------------------------------------------------------
            // COMPARISON REPORT
            // ----------------------------------------------------------------
            println!("\n📊 FINAL RESULTS (Lower is Better)");
            println!("+----------------+----------------+----------------+---------+");
            println!("| Metric         | RustMemoDB     | PostgreSQL     | Winner  |");
            println!("+----------------+----------------+----------------+---------+");
            print_row("Insert (100k)", duration_mem_insert, dur_pg_insert);
            print_row("Select (10k)", duration_mem_select, dur_pg_select);
            print_row("Aggr (Full)", duration_mem_aggr, dur_pg_aggr);
            println!("+----------------+----------------+----------------+---------+");
        }
        Err(e) => {
            println!("⚠️  Skipping PostgreSQL benchmark: {}", e);
            println!("   (Make sure Postgres is running on localhost:5432 with user 'postgres' and pass 'postgres')");
        }
    }

    Ok(())
}

fn print_row(name: &str, mem: Duration, pg: Duration) {
    let winner = if mem < pg { "RustMemoDB" } else { "PostgreSQL" };
    let ratio = if mem < pg {
        pg.as_secs_f64() / mem.as_secs_f64()
    } else {
        mem.as_secs_f64() / pg.as_secs_f64()
    };
    
    println!("| {:<14} | {:>12.2?} | {:>12.2?} | {:<3} {:.1}x |", 
        name, mem, pg, if winner == "RustMemoDB" { "🦀" } else { "🐘" }, ratio);
}

struct BenchRow {
    id: i64,
    val: i64,
    info: String,
}

fn generate_data(count: usize) -> Vec<BenchRow> {
    let mut rng = rand::rng();
    (0..count).map(|i| BenchRow {
        id: i as i64,
        val: rng.random_range(0..count as i64),
        info: format!("data_{}", i),
    }).collect()
}

async fn run_postgres_bench(data: &[BenchRow]) -> std::result::Result<(Duration, Duration, Duration), Box<dyn std::error::Error>> {
    // Connect
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=postgres", 
        NoTls
    ).await?;

    // Spawn the connection object in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Setup
    client.execute("DROP TABLE IF EXISTS bench", &[]).await?;
    client.execute("CREATE TABLE bench (id BIGINT PRIMARY KEY, val BIGINT, info TEXT)", &[]).await?;
    // Note: Creating index AFTER insert is usually faster in PG, but to be fair to RustMemoDB 
    // (which maintained index during insert), we create it now or just create it after to match logic?
    // RustMemoDB updated index on fly. Let's create index now to stress Insert performance too.
    client.execute("CREATE INDEX idx_val_pg ON bench (val)", &[]).await?;

    // 1. Bulk Insert
    // Using PREPARE statement to be efficient (fair comparison against parsed SQL?)
    // Actually, RustMemoDB parses every SQL string. So we should send SQL strings to PG to be perfectly fair (overhead of parsing).
    // BUT, typical PG usage uses prepared statements. Let's use simple query execution to include network + parsing overhead.
    
    let start_insert = Instant::now();
    
    // To avoid 100k network roundtrips which would crush PG performance unfairly (pipeline mode is complex),
    // we will use a reasonable batch size or just prepared statements.
    // Let's use prepared statement to give PG a fighting chance, otherwise it's 100x slower just on RTT.
    let stmt = client.prepare("INSERT INTO bench VALUES ($1, $2, $3)").await?;
    
    for row in data {
        client.execute(&stmt, &[&row.id, &row.val, &row.info]).await?;
    }
    let dur_insert = start_insert.elapsed();

    // 2. Point Select
    let start_select = Instant::now();
    let stmt_select = client.prepare("SELECT * FROM bench WHERE val = $1").await?;
    let mut rng = rand::rng();
    for _ in 0..SELECT_ITERATIONS {
        let target_val = rng.random_range(0..ROW_COUNT as i64);
        let _rows = client.query(&stmt_select, &[&target_val]).await?;
    }
    let dur_select = start_select.elapsed();

    // 3. Aggregation
    let start_aggr = Instant::now();
    client.query("SELECT AVG(val) FROM bench", &[]).await?;
    let dur_aggr = start_aggr.elapsed();

    Ok((dur_insert, dur_select, dur_aggr))
}
