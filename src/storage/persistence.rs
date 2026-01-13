//! Write-Ahead Logging (WAL) and persistence layer for RustMemDB

use crate::core::{DbError, Result, Row, Snapshot};
use crate::storage::table::{Table, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
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
    Insert { table: String, row: Row },
    Update { table: String, row_index: usize, old_row: Row, new_row: Row },
    Delete { table: String, row_indices: Vec<usize>, deleted_rows: Vec<Row> },
    CreateTable { name: String, schema: TableSchema },
    DropTable { name: String, table: Table },
    CreateIndex { table_name: String, column_name: String },
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
    pub metadata: SnapshotMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub created_at: u64,
    pub row_count: usize,
    pub table_count: usize,
}

impl DatabaseSnapshot {
    pub fn new(tables: HashMap<String, Table>) -> Self {
        let row_count = tables.values().map(|t| t.row_count()).sum();
        let table_count = tables.len();
        let created_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        Self {
            version: 1,
            tables,
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
    wal_file: Option<BufWriter<File>>,
    durability_mode: DurabilityMode,
    entries_since_checkpoint: usize,
    checkpoint_threshold: usize,
}

impl WalManager {
    pub fn new<P: AsRef<Path>>(wal_path: P, durability_mode: DurabilityMode) -> Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();
        if let Some(parent) = wal_path.parent() {
            fs::create_dir_all(parent).map_err(|e| DbError::ExecutionError(format!("Failed to create WAL directory: {}", e)))?;
        }

        let wal_file = if durability_mode != DurabilityMode::None {
            let file = OpenOptions::new().create(true).append(true).open(&wal_path)
                .map_err(|e| DbError::ExecutionError(format!("Failed to open WAL file: {}", e)))?;
            Some(BufWriter::new(file))
        } else {
            None
        };

        Ok(Self {
            wal_path,
            wal_file,
            durability_mode,
            entries_since_checkpoint: 0,
            checkpoint_threshold: 1000,
        })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        if self.durability_mode == DurabilityMode::None { return Ok(()); }
        let file = self.wal_file.as_mut().ok_or_else(|| DbError::ExecutionError("WAL file not initialized".to_string()))?;
        let serialized = rmp_serde::to_vec(entry).map_err(|e| DbError::ExecutionError(format!("Failed to serialize WAL entry: {}", e)))?;
        let len = serialized.len() as u32;
        file.write_all(&len.to_le_bytes()).map_err(|e| DbError::ExecutionError(format!("Failed to write WAL: {}", e)))?;
        file.write_all(&serialized).map_err(|e| DbError::ExecutionError(format!("Failed to write WAL: {}", e)))?;
        file.flush().map_err(|e| DbError::ExecutionError(format!("Failed to flush WAL: {}", e)))?;
        if self.durability_mode == DurabilityMode::Sync {
            file.get_mut().sync_all().map_err(|e| DbError::ExecutionError(format!("Failed to sync WAL: {}", e)))?;
        }
        self.entries_since_checkpoint += 1;
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
        self.wal_file = None;
        let file = OpenOptions::new().write(true).truncate(true).open(&self.wal_path)
            .map_err(|e| DbError::ExecutionError(format!("Failed to truncate WAL: {}", e)))?;
        self.wal_file = Some(BufWriter::new(file));
        self.entries_since_checkpoint = 0;
        Ok(())
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.entries_since_checkpoint >= self.checkpoint_threshold
    }

    pub fn entries_since_checkpoint(&self) -> usize {
        self.entries_since_checkpoint
    }

    pub fn set_checkpoint_threshold(&mut self, threshold: usize) {
        self.checkpoint_threshold = threshold;
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

    pub fn checkpoint(&mut self, tables: &HashMap<String, Table>) -> Result<()> {
        if self.durability_mode == DurabilityMode::None { return Ok(()); }
        let snapshot = DatabaseSnapshot::new(tables.clone());
        self.snapshot.save(&snapshot)?;
        self.wal.clear()?;
        Ok(())
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.wal.needs_checkpoint()
    }

    pub fn recover(&self) -> Result<Option<HashMap<String, Table>>> {
        let mut tables = if let Some(snapshot) = self.snapshot.load()? {
            snapshot.tables
        } else {
            HashMap::new()
        };

        let wal_entries = self.wal.read_all()?;
        if tables.is_empty() && wal_entries.is_empty() { return Ok(None); }

        // Use a dummy snapshot for applying WAL entries (committed/system)
        let snapshot = Snapshot {
            tx_id: 0,
            active: Arc::new(HashSet::new()),
            aborted: Arc::new(HashSet::new()),
            max_tx_id: u64::MAX,
        };

        for entry in wal_entries {
            match entry {
                WalEntry::Insert { table, row } => {
                    if let Some(tbl) = tables.get_mut(&table) {
                        tbl.insert(row, &snapshot)?; 
                    }
                }
                WalEntry::Update { table, row_index, new_row, .. } => {
                    if let Some(tbl) = tables.get_mut(&table) {
                        tbl.update(row_index, new_row, &snapshot)?;
                    }
                }
                WalEntry::Delete { table, row_indices, .. } => {
                    if let Some(tbl) = tables.get_mut(&table) {
                        for idx in row_indices {
                            tbl.delete(idx, 0)?; // Delete uses tx_id directly, not snapshot
                        }
                    }
                }
                WalEntry::CreateTable { name, schema } => {
                    let table = Table::new(schema);
                    tables.insert(name, table);
                }
                WalEntry::DropTable { name, .. } => {
                    tables.remove(&name);
                }
                WalEntry::CreateIndex { table_name, column_name } => {
                    if let Some(tbl) = tables.get_mut(&table_name) {
                        let _ = tbl.create_index(&column_name);
                    }
                }
                WalEntry::BeginTransaction(_) | WalEntry::Commit(_) | WalEntry::Rollback(_) => {}
            }
        }
        Ok(Some(tables))
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
    use tempfile::TempDir;

    #[test]
    fn test_wal_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let mut wal = WalManager::new(&wal_path, DurabilityMode::Sync).unwrap();
        wal.append(&WalEntry::BeginTransaction(1)).unwrap();
        wal.append(&WalEntry::Insert {
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
        let snapshot = DatabaseSnapshot::new(tables);
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
        persistence.checkpoint(&tables).unwrap();
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
        persistence.log(&WalEntry::CreateTable {
            name: "users".to_string(),
            schema: schema.clone(),
        }).unwrap();
        persistence.log(&WalEntry::Insert {
            table: "users".to_string(),
            row: vec![
                crate::core::Value::Integer(1),
                crate::core::Value::Text("Alice".to_string()),
            ],
        }).unwrap();
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
        persistence.checkpoint(&tables).unwrap();
        let recovered = persistence.recover().unwrap().unwrap();
        assert!(recovered.contains_key("users"));
        // row_count might be different if MVCC tracks versions, but 1 inserted row means 1 logical row
        // Table::row_count returns number of keys (logical rows).
        assert_eq!(recovered.get("users").unwrap().row_count(), 1);
    }
}