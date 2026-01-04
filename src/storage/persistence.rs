//! Write-Ahead Logging (WAL) and persistence layer for RustMemDB
//!
//! This module provides:
//! - Write-Ahead Logging for durability
//! - Periodic snapshots for fast recovery
//! - Crash recovery mechanism
//! - Configurable durability modes (SYNC/ASYNC/NONE)

use crate::core::{DbError, Result, Row};
use crate::storage::table::{Table, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    /// Begin a transaction
    BeginTransaction(u64),

    /// Commit a transaction
    Commit(u64),

    /// Rollback a transaction
    Rollback(u64),

    /// Insert a row into a table
    Insert {
        table: String,
        row: Row,
    },

    /// Update a row in a table
    Update {
        table: String,
        row_index: usize,
        old_row: Row,
        new_row: Row,
    },

    /// Delete rows from a table
    Delete {
        table: String,
        row_indices: Vec<usize>,
        deleted_rows: Vec<Row>,
    },

    /// Create a new table
    CreateTable {
        name: String,
        schema: TableSchema,
    },

    /// Drop a table
    DropTable {
        name: String,
        /// Store table for potential recovery
        table: Table,
    },
}

impl WalEntry {
    /// Get the timestamp when this entry was created (in milliseconds since epoch)
    pub fn timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

// ============================================================================
// Database Snapshot
// ============================================================================

/// Serializable database snapshot
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseSnapshot {
    /// Schema version for forward compatibility
    pub version: u32,

    /// All tables in the database
    pub tables: HashMap<String, Table>,

    /// Snapshot metadata
    pub metadata: SnapshotMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// When this snapshot was created (milliseconds since epoch)
    pub created_at: u64,

    /// Total number of rows across all tables
    pub row_count: usize,

    /// Number of tables
    pub table_count: usize,
}

impl DatabaseSnapshot {
    /// Create a new snapshot from a table map
    pub fn new(tables: HashMap<String, Table>) -> Self {
        let row_count = tables.values().map(|t| t.row_count()).sum();
        let table_count = tables.len();

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            version: 1,
            tables,
            metadata: SnapshotMetadata {
                created_at,
                row_count,
                table_count,
            },
        }
    }
}

// ============================================================================
// Durability Configuration
// ============================================================================

/// Durability mode for WAL operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// Synchronous: fsync after each commit (slow but durable)
    Sync,

    /// Asynchronous: background fsync (fast but risk of data loss on crash)
    Async,

    /// None: in-memory only (current behavior, no persistence)
    None,
}

impl Default for DurabilityMode {
    fn default() -> Self {
        Self::Async
    }
}

// ============================================================================
// WAL Manager
// ============================================================================

/// Manages Write-Ahead Log operations
pub struct WalManager {
    /// Path to WAL file
    wal_path: PathBuf,

    /// WAL file handle
    wal_file: Option<BufWriter<File>>,

    /// Durability mode
    durability_mode: DurabilityMode,

    /// Number of WAL entries since last checkpoint
    entries_since_checkpoint: usize,

    /// Checkpoint threshold (trigger checkpoint after N entries)
    checkpoint_threshold: usize,
}

impl WalManager {
    /// Create a new WAL manager
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        durability_mode: DurabilityMode,
    ) -> Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();

        // Create parent directory if needed
        if let Some(parent) = wal_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                DbError::ExecutionError(format!("Failed to create WAL directory: {}", e))
            })?;
        }

        // Open or create WAL file
        let wal_file = if durability_mode != DurabilityMode::None {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .map_err(|e| {
                    DbError::ExecutionError(format!("Failed to open WAL file: {}", e))
                })?;
            Some(BufWriter::new(file))
        } else {
            None
        };

        Ok(Self {
            wal_path,
            wal_file,
            durability_mode,
            entries_since_checkpoint: 0,
            checkpoint_threshold: 1000, // Default: checkpoint every 1000 entries
        })
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        if self.durability_mode == DurabilityMode::None {
            return Ok(()); // No-op for in-memory mode
        }

        let file = self.wal_file.as_mut().ok_or_else(|| {
            DbError::ExecutionError("WAL file not initialized".to_string())
        })?;

        // Serialize entry using MessagePack
        let serialized = rmp_serde::to_vec(entry).map_err(|e| {
            DbError::ExecutionError(format!("Failed to serialize WAL entry: {}", e))
        })?;

        // Write length prefix (4 bytes) + data
        let len = serialized.len() as u32;
        file.write_all(&len.to_le_bytes())
            .map_err(|e| DbError::ExecutionError(format!("Failed to write WAL: {}", e)))?;
        file.write_all(&serialized)
            .map_err(|e| DbError::ExecutionError(format!("Failed to write WAL: {}", e)))?;

        // Flush to OS buffer
        file.flush()
            .map_err(|e| DbError::ExecutionError(format!("Failed to flush WAL: {}", e)))?;

        // Sync to disk if in SYNC mode
        if self.durability_mode == DurabilityMode::Sync {
            file.get_mut().sync_all().map_err(|e| {
                DbError::ExecutionError(format!("Failed to sync WAL: {}", e))
            })?;
        }

        self.entries_since_checkpoint += 1;

        Ok(())
    }

    /// Read all entries from the WAL
    pub fn read_all(&self) -> Result<Vec<WalEntry>> {
        if !self.wal_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.wal_path).map_err(|e| {
            DbError::ExecutionError(format!("Failed to open WAL for reading: {}", e))
        })?;

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            // Read length prefix
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(DbError::ExecutionError(format!(
                        "Failed to read WAL entry length: {}",
                        e
                    )))
                }
            }

            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read entry data
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data).map_err(|e| {
                DbError::ExecutionError(format!("Failed to read WAL entry data: {}", e))
            })?;

            // Deserialize entry
            let entry: WalEntry = rmp_serde::from_slice(&data).map_err(|e| {
                DbError::ExecutionError(format!("Failed to deserialize WAL entry: {}", e))
            })?;

            entries.push(entry);
        }

        Ok(entries)
    }

    /// Clear the WAL file (after checkpoint)
    pub fn clear(&mut self) -> Result<()> {
        if self.durability_mode == DurabilityMode::None {
            return Ok(());
        }

        // Close current file
        self.wal_file = None;

        // Truncate file
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.wal_path)
            .map_err(|e| {
                DbError::ExecutionError(format!("Failed to truncate WAL: {}", e))
            })?;

        // Reopen for appending
        self.wal_file = Some(BufWriter::new(file));
        self.entries_since_checkpoint = 0;

        Ok(())
    }

    /// Check if checkpoint is needed
    pub fn needs_checkpoint(&self) -> bool {
        self.entries_since_checkpoint >= self.checkpoint_threshold
    }

    /// Get the number of entries since last checkpoint
    pub fn entries_since_checkpoint(&self) -> usize {
        self.entries_since_checkpoint
    }

    /// Set checkpoint threshold
    pub fn set_checkpoint_threshold(&mut self, threshold: usize) {
        self.checkpoint_threshold = threshold;
    }
}

// ============================================================================
// Snapshot Manager
// ============================================================================

/// Manages database snapshots
pub struct SnapshotManager {
    snapshot_path: PathBuf,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new<P: AsRef<Path>>(snapshot_path: P) -> Self {
        Self {
            snapshot_path: snapshot_path.as_ref().to_path_buf(),
        }
    }

    /// Save a database snapshot atomically
    pub fn save(&self, snapshot: &DatabaseSnapshot) -> Result<()> {
        // Create parent directory if needed
        if let Some(parent) = self.snapshot_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                DbError::ExecutionError(format!("Failed to create snapshot directory: {}", e))
            })?;
        }

        // Write to temporary file first (atomic write pattern)
        let temp_path = self.snapshot_path.with_extension("tmp");
        let temp_file = File::create(&temp_path).map_err(|e| {
            DbError::ExecutionError(format!("Failed to create temp file: {}", e))
        })?;

        let mut writer = BufWriter::new(temp_file);

        // Serialize using MessagePack
        let serialized = rmp_serde::to_vec(snapshot).map_err(|e| {
            DbError::ExecutionError(format!("Failed to serialize snapshot: {}", e))
        })?;

        writer.write_all(&serialized).map_err(|e| {
            DbError::ExecutionError(format!("Failed to write snapshot: {}", e))
        })?;

        // Ensure all data is written to disk
        writer.flush().map_err(|e| {
            DbError::ExecutionError(format!("Failed to flush snapshot: {}", e))
        })?;

        writer.get_mut().sync_all().map_err(|e| {
            DbError::ExecutionError(format!("Failed to sync snapshot: {}", e))
        })?;

        // Atomically replace old snapshot with new one
        fs::rename(&temp_path, &self.snapshot_path).map_err(|e| {
            DbError::ExecutionError(format!("Failed to rename snapshot: {}", e))
        })?;

        Ok(())
    }

    /// Load the latest database snapshot
    pub fn load(&self) -> Result<Option<DatabaseSnapshot>> {
        if !self.snapshot_path.exists() {
            return Ok(None);
        }

        let mut file = File::open(&self.snapshot_path).map_err(|e| {
            DbError::ExecutionError(format!("Failed to open snapshot: {}", e))
        })?;

        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(|e| {
            DbError::ExecutionError(format!("Failed to read snapshot: {}", e))
        })?;

        let snapshot: DatabaseSnapshot = rmp_serde::from_slice(&data).map_err(|e| {
            DbError::ExecutionError(format!("Failed to deserialize snapshot: {}", e))
        })?;

        Ok(Some(snapshot))
    }

    /// Check if a snapshot exists
    pub fn exists(&self) -> bool {
        self.snapshot_path.exists()
    }

    /// Delete the snapshot file
    pub fn delete(&self) -> Result<()> {
        if self.snapshot_path.exists() {
            fs::remove_file(&self.snapshot_path).map_err(|e| {
                DbError::ExecutionError(format!("Failed to delete snapshot: {}", e))
            })?;
        }
        Ok(())
    }
}

// ============================================================================
// Persistence Manager (combines WAL + Snapshots)
// ============================================================================

/// High-level persistence manager
pub struct PersistenceManager {
    wal: WalManager,
    snapshot: SnapshotManager,
    durability_mode: DurabilityMode,
}

impl PersistenceManager {
    /// Create a new persistence manager
    pub fn new<P: AsRef<Path>>(
        data_dir: P,
        durability_mode: DurabilityMode,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();

        let wal_path = data_dir.join("rustmemodb.wal");
        let snapshot_path = data_dir.join("rustmemodb.snapshot");

        let wal = WalManager::new(wal_path, durability_mode)?;
        let snapshot = SnapshotManager::new(snapshot_path);

        Ok(Self {
            wal,
            snapshot,
            durability_mode,
        })
    }

    /// Append a WAL entry
    pub fn log(&mut self, entry: &WalEntry) -> Result<()> {
        self.wal.append(entry)
    }

    /// Create a checkpoint (snapshot + clear WAL)
    pub fn checkpoint(&mut self, tables: &HashMap<String, Table>) -> Result<()> {
        if self.durability_mode == DurabilityMode::None {
            return Ok(());
        }

        // Create snapshot
        let snapshot = DatabaseSnapshot::new(tables.clone());
        self.snapshot.save(&snapshot)?;

        // Clear WAL
        self.wal.clear()?;

        Ok(())
    }

    /// Check if checkpoint is needed
    pub fn needs_checkpoint(&self) -> bool {
        self.wal.needs_checkpoint()
    }

    /// Recover database state from snapshot + WAL
    pub fn recover(&self) -> Result<Option<HashMap<String, Table>>> {
        // Load snapshot if exists, otherwise start with empty database
        let mut tables = if let Some(snapshot) = self.snapshot.load()? {
            snapshot.tables
        } else {
            HashMap::new()
        };

        // Replay WAL entries (even if no snapshot exists)
        let wal_entries = self.wal.read_all()?;

        // If no snapshot and no WAL entries, return None (empty database)
        if tables.is_empty() && wal_entries.is_empty() {
            return Ok(None);
        }

        for entry in wal_entries {
            match entry {
                WalEntry::Insert { table, row } => {
                    if let Some(tbl) = tables.get_mut(&table) {
                        tbl.insert(row)?;
                    }
                }

                WalEntry::Update {
                    table,
                    row_index,
                    new_row,
                    ..
                } => {
                    if let Some(tbl) = tables.get_mut(&table) {
                        tbl.update_row(row_index, new_row)?;
                    }
                }

                WalEntry::Delete { table, row_indices, .. } => {
                    if let Some(tbl) = tables.get_mut(&table) {
                        tbl.delete_rows(row_indices)?;
                    }
                }

                WalEntry::CreateTable { name, schema } => {
                    let table = Table::new(schema);
                    tables.insert(name, table);
                }

                WalEntry::DropTable { name, .. } => {
                    tables.remove(&name);
                }

                // Transaction markers don't modify data
                WalEntry::BeginTransaction(_) |
                WalEntry::Commit(_) |
                WalEntry::Rollback(_) => {}
            }
        }

        Ok(Some(tables))
    }

    /// Get WAL manager (for testing)
    pub fn wal(&self) -> &WalManager {
        &self.wal
    }

    /// Get WAL manager mutably
    pub fn wal_mut(&mut self) -> &mut WalManager {
        &mut self.wal
    }

    /// Get snapshot manager
    pub fn snapshot(&self) -> &SnapshotManager {
        &self.snapshot
    }

    /// Get durability mode
    pub fn durability_mode(&self) -> DurabilityMode {
        self.durability_mode
    }
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

        // Append some entries
        wal.append(&WalEntry::BeginTransaction(1)).unwrap();
        wal.append(&WalEntry::Insert {
            table: "users".to_string(),
            row: vec![crate::core::Value::Integer(1), crate::core::Value::Text("Alice".to_string())],
        }).unwrap();
        wal.append(&WalEntry::Commit(1)).unwrap();

        // Read back
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_snapshot_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot_path = temp_dir.path().join("test.snapshot");

        let snapshot_mgr = SnapshotManager::new(&snapshot_path);

        // Create a test snapshot
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

        // Save
        snapshot_mgr.save(&snapshot).unwrap();
        assert!(snapshot_mgr.exists());

        // Load
        let loaded = snapshot_mgr.load().unwrap().unwrap();
        assert_eq!(loaded.metadata.table_count, 1);
        assert!(loaded.tables.contains_key("users"));
    }

    #[test]
    fn test_checkpoint_clears_wal() {
        let temp_dir = TempDir::new().unwrap();

        let mut persistence = PersistenceManager::new(
            temp_dir.path(),
            DurabilityMode::Sync,
        ).unwrap();

        // Log some entries
        persistence.log(&WalEntry::BeginTransaction(1)).unwrap();
        persistence.log(&WalEntry::Commit(1)).unwrap();

        assert_eq!(persistence.wal().entries_since_checkpoint(), 2);

        // Checkpoint
        let tables = HashMap::new();
        persistence.checkpoint(&tables).unwrap();

        assert_eq!(persistence.wal().entries_since_checkpoint(), 0);
    }

    #[test]
    fn test_recovery() {
        let temp_dir = TempDir::new().unwrap();

        let mut persistence = PersistenceManager::new(
            temp_dir.path(),
            DurabilityMode::Sync,
        ).unwrap();

        // Create table
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

        // Insert data
        persistence.log(&WalEntry::Insert {
            table: "users".to_string(),
            row: vec![
                crate::core::Value::Integer(1),
                crate::core::Value::Text("Alice".to_string()),
            ],
        }).unwrap();

        // Create checkpoint
        let mut tables = HashMap::new();
        tables.insert("users".to_string(), {
            let mut table = Table::new(schema);
            table.insert(vec![
                crate::core::Value::Integer(1),
                crate::core::Value::Text("Alice".to_string()),
            ]).unwrap();
            table
        });

        persistence.checkpoint(&tables).unwrap();

        // Now recover
        let recovered = persistence.recover().unwrap().unwrap();
        assert!(recovered.contains_key("users"));
        assert_eq!(recovered.get("users").unwrap().row_count(), 1);
    }
}
