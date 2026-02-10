#![allow(dead_code)]

use chrono::{TimeZone, Utc};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_THRESHOLD_PCT: f64 = 25.0;
const DEFAULT_TIMEOUT_MS: u64 = 60000;

#[derive(Debug, Clone)]
pub struct PerfConfig {
    pub run_dir: PathBuf,
    pub history_path: PathBuf,
    pub run_path: PathBuf,
    pub baseline_path: PathBuf,
    pub threshold_pct: f64,
    pub timeout: Duration,
    pub accept_baseline: bool,
    pub run_start_ms: u128,
}

impl PerfConfig {
    pub fn from_env() -> Self {
        let run_dir = std::env::var("RUSTMEMODB_PERF_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("tests/perf"));
        let baseline_path = std::env::var("RUSTMEMODB_PERF_BASELINE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("tests/perf/baseline.csv"));
        let threshold_pct = std::env::var("RUSTMEMODB_PERF_THRESHOLD_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(DEFAULT_THRESHOLD_PCT);
        let timeout_ms = std::env::var("RUSTMEMODB_PERF_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        let accept_baseline = std::env::var("RUSTMEMODB_PERF_WRITE_BASELINE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let history_path = run_dir.join("history.csv");
        let run_start_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_millis(0))
            .as_millis();
        let run_path = run_dir.join(format!("run_{}.csv", run_start_ms));

        Self {
            run_dir,
            history_path,
            run_path,
            baseline_path,
            threshold_pct,
            timeout: Duration::from_millis(timeout_ms),
            accept_baseline,
            run_start_ms,
        }
    }
}

pub fn start_run() -> io::Result<PerfConfig> {
    let cfg = PerfConfig::from_env();
    fs::create_dir_all(&cfg.run_dir)?;
    Ok(cfg)
}

pub fn record_metric(cfg: &PerfConfig, name: &str, duration: Duration) -> io::Result<()> {
    let ms = duration.as_secs_f64() * 1000.0;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.run_path)?;
    writeln!(file, "{},{}", name, format!("{:.3}", ms))?;
    Ok(())
}

pub fn finalize_run(cfg: &PerfConfig) -> io::Result<()> {
    let end_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_millis(0))
        .as_millis();
    let start_iso = Utc
        .timestamp_millis_opt(cfg.run_start_ms as i64)
        .single()
        .map(|v| v.to_rfc3339());
    let end_iso = Utc
        .timestamp_millis_opt(end_ms as i64)
        .single()
        .map(|v| v.to_rfc3339());
    let metrics = read_metrics(&cfg.run_path)?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.history_path)?;
    for (name, value) in metrics {
        writeln!(
            file,
            "{},{},{},{}",
            start_iso.as_deref().unwrap_or(""),
            end_iso.as_deref().unwrap_or(""),
            name,
            format!("{:.3}ms", value)
        )?;
    }
    Ok(())
}

pub fn read_metrics(path: &Path) -> io::Result<BTreeMap<String, f64>> {
    let content = fs::read_to_string(path)?;
    let mut map = BTreeMap::new();
    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, ',');
        let name = parts
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid metric name at line {}", idx + 1),
                )
            })?;
        let value = parts.next().map(str::trim).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Missing metric value at line {}", idx + 1),
            )
        })?;
        let ms = value.parse::<f64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid metric value at line {}", idx + 1),
            )
        })?;
        map.insert(name.to_string(), ms);
    }
    Ok(map)
}

pub fn read_history(path: &Path) -> io::Result<BTreeMap<String, BTreeMap<String, f64>>> {
    let content = fs::read_to_string(path)?;
    let mut runs: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(7, ',').collect();
        if parts.len() != 4 && parts.len() != 7 && parts.len() != 5 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid history line at {}", idx + 1),
            ));
        }
        let run_id = if parts.len() == 4 {
            parts[0].trim().to_string()
        } else {
            parts[0].trim().to_string()
        };
        let metric = if parts.len() == 4 {
            parts[2].trim()
        } else if parts.len() == 7 {
            parts[5].trim()
        } else {
            parts[3].trim()
        };
        if metric.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid metric name at line {}", idx + 1),
            ));
        }
        let value = if parts.len() == 4 {
            parts[3].trim()
        } else if parts.len() == 7 {
            parts[6].trim()
        } else {
            parts[4].trim()
        };
        let value = value.trim_end_matches("ms").parse::<f64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid metric value at line {}", idx + 1),
            )
        })?;
        runs.entry(run_id)
            .or_default()
            .insert(metric.to_string(), value);
    }
    Ok(runs)
}

pub fn compare_metrics(
    baseline: &BTreeMap<String, f64>,
    latest: &BTreeMap<String, f64>,
    threshold_pct: f64,
) -> Vec<String> {
    let mut regressions = Vec::new();
    for (name, base_ms) in baseline {
        let Some(latest_ms) = latest.get(name) else {
            regressions.push(format!("Metric '{}' missing in latest run", name));
            continue;
        };
        let limit = base_ms * (1.0 + threshold_pct / 100.0);
        if *latest_ms > limit {
            regressions.push(format!(
                "{}: baseline {:.3} ms, latest {:.3} ms (limit {:.3} ms)",
                name, base_ms, latest_ms, limit
            ));
        }
    }
    regressions
}

pub fn write_baseline(cfg: &PerfConfig, latest: &BTreeMap<String, f64>) -> io::Result<()> {
    if let Some(parent) = cfg.baseline_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(&cfg.baseline_path)?;
    for (name, value) in latest {
        writeln!(file, "{},{}", name, format!("{:.3}", value))?;
    }
    Ok(())
}
