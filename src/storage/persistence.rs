//! Write-Ahead Logging (WAL) and persistence layer for RustMemDB

use crate::core::{DbError, Result, Row, Snapshot, Column};
use crate::storage::table::{Table, TableSchema};
use crate::parser::ast::{QueryStmt, AlterTableOperation};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// WAL Entry Types
// ============================================================================

/// Write-Ahead Log entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    BeginTransaction(u64),
    Commit(u64),
    Rollback(u64),
    Insert { tx_id: u64, table: String, row: Row },
    Update { tx_id: u64, table: String, row_index: usize, old_row: Row, new_row: Row },
    Delete { tx_id: u64, table: String, row_indices: Vec<usize>, deleted_rows: Vec<Row> },
    CreateTable { tx_id: u64, name: String, schema: TableSchema },
    DropTable { tx_id: u64, name: String, table: Table },
    CreateIndex { tx_id: u64, table_name: String, column_name: String },
    CreateView { tx_id: u64, name: String, query: QueryStmt, columns: Vec<String>, or_replace: bool },
    DropView { tx_id: u64, name: String },
    RenameTable { tx_id: u64, old_name: String, new_name: String },
    AlterTable { tx_id: u64, table_name: String, operation: AlterTableOperation },
}

impl WalEntry {
    pub fn timestamp(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }
}

// ============================================================================
// Database Snapshot
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseSnapshot {
    pub version: u32,
    pub tables: HashMap<String, Table>,
    pub views: HashMap<String, (QueryStmt, Vec<String>)>,
    pub metadata: SnapshotMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub created_at: u64,
    pub row_count: usize,
    pub table_count: usize,
}

impl DatabaseSnapshot {
    pub fn new(tables: HashMap<String, Table>, views: HashMap<String, (QueryStmt, Vec<String>)>) -> Self {
        let row_count = tables.values().map(|t| t.row_count()).sum();
        let table_count = tables.len();
        let created_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        Self {
            version: 1,
            tables,
            views,
            metadata: SnapshotMetadata { created_at, row_count, table_count },
        }
    }
}

// ============================================================================
// Durability Configuration
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum DurabilityMode {
    Sync,
    #[default]
    Async,
    None,
}


// ============================================================================
// WAL Manager
// ============================================================================

pub struct WalManager {
    wal_path: PathBuf,
    durability_mode: DurabilityMode,
    entries_since_checkpoint: AtomicUsize,
    checkpoint_threshold: usize,
    writer: Option<WalWriter>,
    metrics: Arc<WalMetrics>,
}

impl WalManager {
    pub fn new<P: AsRef<Path>>(wal_path: P, durability_mode: DurabilityMode) -> Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();
        if let Some(parent) = wal_path.parent() {
            fs::create_dir_all(parent).map_err(|e| DbError::ExecutionError(format!("Failed to create WAL directory: {}", e)))?;
        }

        let metrics = Arc::new(WalMetrics::default());
        let writer = if durability_mode != DurabilityMode::None {
            Some(WalWriter::start(wal_path.clone(), durability_mode, Arc::clone(&metrics))?)
        } else {
            None
        };

        Ok(Self {
            wal_path,
            durability_mode,
            entries_since_checkpoint: AtomicUsize::new(0),
            checkpoint_threshold: 1000,
            writer,
            metrics,
        })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        if self.durability_mode == DurabilityMode::None { return Ok(()); }
        let serialized = rmp_serde::to_vec(entry)
            .map_err(|e| DbError::ExecutionError(format!("Failed to serialize WAL entry: {}", e)))?;
        let len = serialized.len() as u32;
        let mut payload = Vec::with_capacity(4 + serialized.len());
        payload.extend_from_slice(&len.to_le_bytes());
        payload.extend_from_slice(&serialized);

        if let Some(writer) = &self.writer {
            let is_commit = matches!(entry, WalEntry::Commit(_));
            let wait_for_sync = is_commit && self.durability_mode == DurabilityMode::Sync;
            writer.append(payload, is_commit, wait_for_sync)?;
            self.metrics.on_append(serialized.len() as u64, is_commit);
            self.entries_since_checkpoint.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<WalEntry>> {
        if !self.wal_path.exists() { return Ok(Vec::new()); }
        let file = File::open(&self.wal_path).map_err(|e| DbError::ExecutionError(format!("Failed to open WAL for reading: {}", e)))?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(DbError::ExecutionError(format!("Failed to read WAL entry length: {}", e))),
            }
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data).map_err(|e| DbError::ExecutionError(format!("Failed to read WAL entry data: {}", e)))?;
            let entry: WalEntry = rmp_serde::from_slice(&data).map_err(|e| DbError::ExecutionError(format!("Failed to deserialize WAL entry: {}", e)))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    pub fn clear(&mut self) -> Result<()> {
        if self.durability_mode == DurabilityMode::None { return Ok(()); }
        if let Some(writer) = &self.writer {
            writer.truncate()?;
        }
        self.entries_since_checkpoint.store(0, Ordering::Relaxed);
        Ok(())
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.entries_since_checkpoint.load(Ordering::Relaxed) >= self.checkpoint_threshold
    }

    pub fn entries_since_checkpoint(&self) -> usize {
        self.entries_since_checkpoint.load(Ordering::Relaxed)
    }

    pub fn set_checkpoint_threshold(&mut self, threshold: usize) {
        self.checkpoint_threshold = threshold;
    }

    pub fn metrics(&self) -> WalMetricsSnapshot {
        self.metrics.snapshot()
    }
}

#[derive(Default)]
pub struct WalMetrics {
    bytes_written: AtomicU64,
    entries_written: AtomicU64,
    commit_entries: AtomicU64,
    flush_count: AtomicU64,
    sync_count: AtomicU64,
}

impl WalMetrics {
    fn on_append(&self, bytes: u64, is_commit: bool) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.entries_written.fetch_add(1, Ordering::Relaxed);
        if is_commit {
            self.commit_entries.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_flush(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_sync(&self) {
        self.sync_count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> WalMetricsSnapshot {
        WalMetricsSnapshot {
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            entries_written: self.entries_written.load(Ordering::Relaxed),
            commit_entries: self.commit_entries.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            sync_count: self.sync_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalMetricsSnapshot {
    pub bytes_written: u64,
    pub entries_written: u64,
    pub commit_entries: u64,
    pub flush_count: u64,
    pub sync_count: u64,
}

enum WalCommand {
    Append { bytes: Vec<u8>, is_commit: bool, ack: Option<Sender<()>> },
    Flush { ack: Sender<()> },
    Truncate { ack: Sender<()> },
    Shutdown,
}

struct WalWriter {
    sender: Sender<WalCommand>,
    join: Option<thread::JoinHandle<()>>,
    metrics: Arc<WalMetrics>,
}

impl WalWriter {
    fn start(path: PathBuf, durability: DurabilityMode, metrics: Arc<WalMetrics>) -> Result<Self> {
        let (tx, rx) = mpsc::channel();
        let metrics_clone = Arc::clone(&metrics);

        let join = thread::Builder::new()
            .name("wal-writer".to_string())
            .spawn(move || wal_writer_loop(path, durability, rx, metrics_clone))
            .map_err(|e| DbError::ExecutionError(format!("Failed to start WAL writer: {}", e)))?;

        Ok(Self { sender: tx, join: Some(join), metrics })
    }

    fn append(&self, bytes: Vec<u8>, is_commit: bool, wait_for_sync: bool) -> Result<()> {
        if wait_for_sync {
            let (tx, rx) = mpsc::channel();
            self.sender
                .send(WalCommand::Append { bytes, is_commit, ack: Some(tx) })
                .map_err(|e| DbError::ExecutionError(format!("Failed to send WAL entry: {}", e)))?;
            rx.recv()
                .map_err(|e| DbError::ExecutionError(format!("Failed to wait WAL sync: {}", e)))?;
            return Ok(());
        }

        self.sender
            .send(WalCommand::Append { bytes, is_commit, ack: None })
            .map_err(|e| DbError::ExecutionError(format!("Failed to send WAL entry: {}", e)))?;
        Ok(())
    }

    fn truncate(&self) -> Result<()> {
        let (tx, rx) = mpsc::channel();
        self.sender
            .send(WalCommand::Truncate { ack: tx })
            .map_err(|e| DbError::ExecutionError(format!("Failed to truncate WAL: {}", e)))?;
        rx.recv()
            .map_err(|e| DbError::ExecutionError(format!("Failed to truncate WAL: {}", e)))?;
        Ok(())
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        let _ = self.sender.send(WalCommand::Shutdown);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn wal_writer_loop(path: PathBuf, durability: DurabilityMode, rx: Receiver<WalCommand>, metrics: Arc<WalMetrics>) {
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => BufWriter::new(f),
        Err(_) => return,
    };

    let mut pending_commit_acks: Vec<Sender<()>> = Vec::new();
    let mut last_flush = Instant::now();
    let flush_interval = Duration::from_millis(50);
    let commit_window = Duration::from_millis(5);

    loop {
        let cmd = match rx.recv() {
            Ok(cmd) => cmd,
            Err(_) => break,
        };

        if matches!(cmd, WalCommand::Shutdown) {
            break;
        }

        let mut commit_pending = false;
        let mut drain_deadline = None::<Instant>;

        let mut process_cmd = |cmd: WalCommand,
                               file: &mut BufWriter<File>,
                               pending_commit_acks: &mut Vec<Sender<()>>,
                               commit_pending: &mut bool| {
            match cmd {
                WalCommand::Append { bytes, is_commit, ack } => {
                    let _ = file.write_all(&bytes);
                    if is_commit {
                        *commit_pending = true;
                        if let Some(ack) = ack {
                            pending_commit_acks.push(ack);
                        }
                    }
                }
                WalCommand::Flush { ack } => {
                    let _ = file.flush();
                    metrics.on_flush();
                    let _ = ack.send(());
                }
                WalCommand::Truncate { ack } => {
                    let _ = file.flush();
                    metrics.on_flush();
                    let _ = file.get_mut().sync_all();
                    metrics.on_sync();
                    if let Ok(new_file) = OpenOptions::new().write(true).truncate(true).open(&path) {
                        *file = BufWriter::new(new_file);
                    }
                    let _ = ack.send(());
                }
                WalCommand::Shutdown => {}
            }
        };

        process_cmd(cmd, &mut file, &mut pending_commit_acks, &mut commit_pending);

        if commit_pending {
            let mut saw_extra = false;
            loop {
                match rx.try_recv() {
                    Ok(cmd) => {
                        if matches!(cmd, WalCommand::Shutdown) {
                            return;
                        }
                        saw_extra = true;
                        process_cmd(cmd, &mut file, &mut pending_commit_acks, &mut commit_pending);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            if saw_extra {
                drain_deadline = Some(Instant::now() + commit_window);
            }
        }

        while let Some(deadline) = drain_deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match rx.recv_timeout(remaining) {
                Ok(cmd) => {
                    if matches!(cmd, WalCommand::Shutdown) {
                        return;
                    }
                    process_cmd(cmd, &mut file, &mut pending_commit_acks, &mut commit_pending);
                }
                Err(_) => break,
            }
        }

        let now = Instant::now();
        let should_flush = commit_pending || now.duration_since(last_flush) >= flush_interval;
        if should_flush {
            let _ = file.flush();
            metrics.on_flush();
            if commit_pending && durability == DurabilityMode::Sync {
                let _ = file.get_mut().sync_all();
                metrics.on_sync();
            }
            last_flush = now;
        }

        if commit_pending {
            for ack in pending_commit_acks.drain(..) {
                let _ = ack.send(());
            }
        }
    }
}

// ============================================================================
// Snapshot Manager
// ============================================================================

pub struct SnapshotManager {
    snapshot_path: PathBuf,
}

impl SnapshotManager {
    pub fn new<P: AsRef<Path>>(snapshot_path: P) -> Self {
        Self {
            snapshot_path: snapshot_path.as_ref().to_path_buf(),
        }
    }

    pub fn save(&self, snapshot: &DatabaseSnapshot) -> Result<()> {
        if let Some(parent) = self.snapshot_path.parent() {
            fs::create_dir_all(parent).map_err(|e| DbError::ExecutionError(format!("Failed to create snapshot directory: {}", e)))?;
        }
        let temp_path = self.snapshot_path.with_extension("tmp");
        let temp_file = File::create(&temp_path).map_err(|e| DbError::ExecutionError(format!("Failed to create temp file: {}", e)))?;
        let mut writer = BufWriter::new(temp_file);
        let serialized = rmp_serde::to_vec(snapshot).map_err(|e| DbError::ExecutionError(format!("Failed to serialize snapshot: {}", e)))?;
        writer.write_all(&serialized).map_err(|e| DbError::ExecutionError(format!("Failed to write snapshot: {}", e)))?;
        writer.flush().map_err(|e| DbError::ExecutionError(format!("Failed to flush snapshot: {}", e)))?;
        writer.get_mut().sync_all().map_err(|e| DbError::ExecutionError(format!("Failed to sync snapshot: {}", e)))?;
        fs::rename(&temp_path, &self.snapshot_path).map_err(|e| DbError::ExecutionError(format!("Failed to rename snapshot: {}", e)))?;
        Ok(())
    }

    pub fn load(&self) -> Result<Option<DatabaseSnapshot>> {
        if !self.snapshot_path.exists() { return Ok(None); }
        let mut file = File::open(&self.snapshot_path).map_err(|e| DbError::ExecutionError(format!("Failed to open snapshot: {}", e)))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(|e| DbError::ExecutionError(format!("Failed to read snapshot: {}", e)))?;
        let snapshot: DatabaseSnapshot = rmp_serde::from_slice(&data).map_err(|e| DbError::ExecutionError(format!("Failed to deserialize snapshot: {}", e)))?;
        Ok(Some(snapshot))
    }

    pub fn exists(&self) -> bool {
        self.snapshot_path.exists()
    }

    pub fn delete(&self) -> Result<()> {
        if self.snapshot_path.exists() {
            fs::remove_file(&self.snapshot_path).map_err(|e| DbError::ExecutionError(format!("Failed to delete snapshot: {}", e)))?;
        }
        Ok(())
    }
}

// ============================================================================
// Persistence Manager
// ============================================================================

fn apply_alter_table(table: &mut Table, operation: AlterTableOperation) -> Result<()> {
    match operation {
        AlterTableOperation::AddColumn(col_def) => {
            let mut column = Column::new(col_def.name, col_def.data_type);
            if !col_def.nullable {
                column = column.not_null();
            }
            if col_def.primary_key {
                column = column.primary_key();
            }
            if col_def.unique {
                column = column.unique();
            }
            if let Some(ref fk) = col_def.references {
                column = column.references(fk.table.clone(), fk.column.clone());
            }
            column.default = col_def.default.clone();
            table.add_column(column, col_def.check.clone())?;
        }
        AlterTableOperation::DropColumn(col_name) => {
            table.drop_column(&col_name)?;
        }
        AlterTableOperation::RenameColumn { old_name, new_name } => {
            table.rename_column(&old_name, &new_name)?;
        }
        AlterTableOperation::RenameTable(_) => {
            return Err(DbError::UnsupportedOperation("Rename table is not supported in AlterTable recovery".into()));
        }
    }
    Ok(())
}

pub struct PersistenceManager {
    wal: WalManager,
    snapshot: SnapshotManager,
    durability_mode: DurabilityMode,
}

impl PersistenceManager {
    pub fn new<P: AsRef<Path>>(data_dir: P, durability_mode: DurabilityMode) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let wal_path = data_dir.join("rustmemodb.wal");
        let snapshot_path = data_dir.join("rustmemodb.snapshot");
        let wal = WalManager::new(wal_path, durability_mode)?;
        let snapshot = SnapshotManager::new(snapshot_path);
        Ok(Self { wal, snapshot, durability_mode })
    }

    pub fn log(&mut self, entry: &WalEntry) -> Result<()> {
        self.wal.append(entry)
    }

    pub fn checkpoint(&mut self, tables: &HashMap<String, Table>, views: &HashMap<String, (QueryStmt, Vec<String>)>) -> Result<()> {
        if self.durability_mode == DurabilityMode::None { return Ok(()); }
        let snapshot = DatabaseSnapshot::new(tables.clone(), views.clone());
        self.snapshot.save(&snapshot)?;
        self.wal.clear()?;
        Ok(())
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.wal.needs_checkpoint()
    }

    pub fn recover(&self) -> Result<Option<DatabaseSnapshot>> {
        let (mut tables, mut views) = if let Some(snapshot) = self.snapshot.load()? {
            (snapshot.tables, snapshot.views)
        } else {
            (HashMap::new(), HashMap::new())
        };

        let wal_entries = self.wal.read_all()?;
        if tables.is_empty() && views.is_empty() && wal_entries.is_empty() { return Ok(None); }

        let mut committed = HashSet::new();
        let mut aborted = HashSet::new();

        for entry in &wal_entries {
            match entry {
                WalEntry::BeginTransaction(_tx_id) => {}
                WalEntry::Commit(tx_id) => {
                    committed.insert(*tx_id);
                }
                WalEntry::Rollback(tx_id) => {
                    aborted.insert(*tx_id);
                }
                _ => {}
            }
        }

        let snapshot_for = |tx_id: u64| Snapshot {
            tx_id,
            active: Arc::new(HashSet::new()),
            aborted: Arc::new(HashSet::new()),
            max_tx_id: u64::MAX,
        };

        for entry in wal_entries {
            match entry {
                WalEntry::Insert { tx_id, table, row } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        if let Some(tbl) = tables.get_mut(&table) {
                            let snapshot = snapshot_for(tx_id);
                            tbl.insert(row, &snapshot)?;
                        }
                    }
                }
                WalEntry::Update { tx_id, table, row_index, new_row, .. } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        if let Some(tbl) = tables.get_mut(&table) {
                            let snapshot = snapshot_for(tx_id);
                            tbl.update(row_index, new_row, &snapshot)?;
                        }
                    }
                }
                WalEntry::Delete { tx_id, table, row_indices, .. } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        if let Some(tbl) = tables.get_mut(&table) {
                            for idx in row_indices {
                                tbl.delete(idx, tx_id)?;
                            }
                        }
                    }
                }
                WalEntry::CreateTable { tx_id, name, schema } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        let table = Table::new(schema);
                        tables.insert(name, table);
                    }
                }
                WalEntry::DropTable { tx_id, name, .. } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        tables.remove(&name);
                    }
                }
                WalEntry::CreateIndex { tx_id, table_name, column_name } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        if let Some(tbl) = tables.get_mut(&table_name) {
                            let _ = tbl.create_index(&column_name);
                        }
                    }
                }
                WalEntry::CreateView { tx_id, name, query, columns, or_replace } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        if or_replace || !views.contains_key(&name) {
                            views.insert(name, (query, columns));
                        }
                    }
                }
                WalEntry::DropView { tx_id, name } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        views.remove(&name);
                    }
                }
                WalEntry::RenameTable { tx_id, old_name, new_name } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        if let Some(table) = tables.remove(&old_name) {
                            tables.insert(new_name, table);
                        }
                    }
                }
                WalEntry::AlterTable { tx_id, table_name, operation } => {
                    if committed.contains(&tx_id) && !aborted.contains(&tx_id) {
                        if let Some(tbl) = tables.get_mut(&table_name) {
                            apply_alter_table(tbl, operation)?;
                        }
                    }
                }
                WalEntry::BeginTransaction(_) | WalEntry::Commit(_) | WalEntry::Rollback(_) => {}
            }
        }
        Ok(Some(DatabaseSnapshot::new(tables, views)))
    }

    pub fn wal(&self) -> &WalManager { &self.wal }
    pub fn wal_mut(&mut self) -> &mut WalManager { &mut self.wal }
    pub fn snapshot(&self) -> &SnapshotManager { &self.snapshot }
    pub fn durability_mode(&self) -> DurabilityMode { self.durability_mode }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Column, DataType};
    use crate::parser::{SqlParserAdapter};
    use crate::parser::ast::Statement;
    use tempfile::TempDir;

    #[test]
    fn test_wal_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let mut wal = WalManager::new(&wal_path, DurabilityMode::Sync).unwrap();
        wal.append(&WalEntry::BeginTransaction(1)).unwrap();
        wal.append(&WalEntry::Insert {
            tx_id: 1,
            table: "users".to_string(),
            row: vec![crate::core::Value::Integer(1), crate::core::Value::Text("Alice".to_string())],
        }).unwrap();
        wal.append(&WalEntry::Commit(1)).unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_snapshot_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("test.snapshot");
        let snapshot_mgr = SnapshotManager::new(&snapshot_path);
        let mut tables = HashMap::new();
        let schema = TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::Integer),
                Column::new("name", DataType::Text),
            ],
        );
        tables.insert("users".to_string(), Table::new(schema));
        let snapshot = DatabaseSnapshot::new(tables, HashMap::new());
        snapshot_mgr.save(&snapshot).unwrap();
        assert!(snapshot_mgr.exists());
        let loaded = snapshot_mgr.load().unwrap().unwrap();
        assert_eq!(loaded.metadata.table_count, 1);
        assert!(loaded.tables.contains_key("users"));
    }

    #[test]
    fn test_checkpoint_clears_wal() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = PersistenceManager::new(temp_dir.path(), DurabilityMode::Sync).unwrap();
        persistence.log(&WalEntry::BeginTransaction(1)).unwrap();
        persistence.log(&WalEntry::Commit(1)).unwrap();
        assert_eq!(persistence.wal().entries_since_checkpoint(), 2);
        let tables = HashMap::new();
        persistence.checkpoint(&tables, &HashMap::new()).unwrap();
        assert_eq!(persistence.wal().entries_since_checkpoint(), 0);
    }

    #[test]
    fn test_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = PersistenceManager::new(temp_dir.path(), DurabilityMode::Sync).unwrap();
        let schema = TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::Integer),
                Column::new("name", DataType::Text),
            ],
        );
        persistence.log(&WalEntry::BeginTransaction(1)).unwrap();
        persistence.log(&WalEntry::CreateTable {
            tx_id: 1,
            name: "users".to_string(),
            schema: schema.clone(),
        }).unwrap();
        persistence.log(&WalEntry::Insert {
            tx_id: 1,
            table: "users".to_string(),
            row: vec![
                crate::core::Value::Integer(1),
                crate::core::Value::Text("Alice".to_string()),
            ],
        }).unwrap();
        persistence.log(&WalEntry::Commit(1)).unwrap();
        let mut tables = HashMap::new();
        tables.insert("users".to_string(), {
            let mut table = Table::new(schema);
            let snapshot = Snapshot {
                tx_id: 0,
                active: Arc::new(HashSet::new()),
                aborted: Arc::new(HashSet::new()),
                max_tx_id: u64::MAX
            };
            table.insert(vec![
                crate::core::Value::Integer(1),
                crate::core::Value::Text("Alice".to_string()),
            ], &snapshot).unwrap();
            table
        });
        persistence.checkpoint(&tables, &HashMap::new()).unwrap();
        let recovered = persistence.recover().unwrap().unwrap();
        assert!(recovered.tables.contains_key("users"));
        // row_count might be different if MVCC tracks versions, but 1 inserted row means 1 logical row
        // Table::row_count returns number of keys (logical rows).
        assert_eq!(recovered.tables.get("users").unwrap().row_count(), 1);
    }

    #[test]
    fn test_recovery_ignores_uncommitted() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = PersistenceManager::new(temp_dir.path(), DurabilityMode::Sync).unwrap();
        let schema = TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::Integer),
                Column::new("name", DataType::Text),
            ],
        );

        persistence.log(&WalEntry::BeginTransaction(1)).unwrap();
        persistence.log(&WalEntry::CreateTable {
            tx_id: 1,
            name: "users".to_string(),
            schema: schema.clone(),
        }).unwrap();
        persistence.log(&WalEntry::Commit(1)).unwrap();

        persistence.log(&WalEntry::BeginTransaction(2)).unwrap();
        persistence.log(&WalEntry::Insert {
            tx_id: 2,
            table: "users".to_string(),
            row: vec![
                crate::core::Value::Integer(1),
                crate::core::Value::Text("Alice".to_string()),
            ],
        }).unwrap();
        persistence.log(&WalEntry::Rollback(2)).unwrap();

        persistence.log(&WalEntry::BeginTransaction(3)).unwrap();
        persistence.log(&WalEntry::Insert {
            tx_id: 3,
            table: "users".to_string(),
            row: vec![
                crate::core::Value::Integer(2),
                crate::core::Value::Text("Bob".to_string()),
            ],
        }).unwrap();
        persistence.log(&WalEntry::Commit(3)).unwrap();

        let recovered = persistence.recover().unwrap().unwrap();
        let table = recovered.tables.get("users").unwrap();
        assert_eq!(table.row_count(), 1);
        let snapshot = Snapshot {
            tx_id: 0,
            active: Arc::new(HashSet::new()),
            aborted: Arc::new(HashSet::new()),
            max_tx_id: u64::MAX,
        };
        let rows = table.scan(&snapshot);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], crate::core::Value::Integer(2));
    }

    #[test]
    fn test_recovery_views() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = PersistenceManager::new(temp_dir.path(), DurabilityMode::Sync).unwrap();

        let parser = SqlParserAdapter::new();
        let stmts = parser.parse("CREATE VIEW v AS SELECT 1").unwrap();
        let Statement::CreateView(create_view) = stmts.into_iter().next().unwrap() else {
            panic!("Expected CREATE VIEW statement");
        };

        persistence.log(&WalEntry::BeginTransaction(1)).unwrap();
        persistence.log(&WalEntry::CreateView {
            tx_id: 1,
            name: create_view.name.clone(),
            query: *create_view.query.clone(),
            columns: create_view.columns.clone(),
            or_replace: create_view.or_replace,
        }).unwrap();
        persistence.log(&WalEntry::Commit(1)).unwrap();

        let recovered = persistence.recover().unwrap().unwrap();
        assert!(recovered.views.contains_key("v"));
    }
}
