use crate::core::{DbError, Result};
use crate::facade::InMemoryDB;
use crate::persist::{
    PersistCommandContract, PersistCommandModel, PersistEntity, PersistPatchContract,
    PersistSession, RestoreConflictPolicy, SnapshotMode,
};
use crate::transaction::TransactionId;
use chrono::Utc;
use log::warn;
use serde::{Serialize, de::DeserializeOwned};
use std::cmp::Ordering;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::fs;

pub trait PersistCollection: Sized + Send + Sync + 'static {
    type Snapshot: Serialize + DeserializeOwned + Send + Sync + 'static;

    fn new_collection(name: impl Into<String>) -> Self;
    fn len(&self) -> usize;
    fn snapshot(&self, mode: SnapshotMode) -> Self::Snapshot;
    fn save_all<'a>(
        &'a mut self,
        session: &'a PersistSession,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    fn restore_with_policy<'a>(
        &'a mut self,
        snapshot: Self::Snapshot,
        session: &'a PersistSession,
        conflict_policy: RestoreConflictPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

pub trait PersistIndexedCollection: PersistCollection {
    type Item: PersistEntity + Send + Sync + 'static;

    fn items(&self) -> &[Self::Item];
    fn items_mut(&mut self) -> &mut [Self::Item];
    fn add_one(&mut self, item: Self::Item);
    fn add_many(&mut self, items: Vec<Self::Item>);
    fn remove_by_persist_id(&mut self, persist_id: &str) -> Option<Self::Item>;
}

#[derive(Debug, Clone)]
pub enum PersistReplicationMode {
    Sync,
    AsyncBestEffort,
}

#[derive(Debug, Clone)]
pub struct PersistReplicationPolicy {
    pub mode: PersistReplicationMode,
    pub replica_roots: Vec<PathBuf>,
}

impl Default for PersistReplicationPolicy {
    fn default() -> Self {
        Self {
            mode: PersistReplicationMode::Sync,
            replica_roots: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistAppPolicy {
    pub snapshot_every_ops: usize,
    pub replication: PersistReplicationPolicy,
}

impl Default for PersistAppPolicy {
    fn default() -> Self {
        Self {
            snapshot_every_ops: 50,
            replication: PersistReplicationPolicy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistAppAutoPolicy {
    pub snapshot_every_ops: usize,
    pub replication: PersistReplicationPolicy,
}

impl Default for PersistAppAutoPolicy {
    fn default() -> Self {
        Self {
            snapshot_every_ops: 1,
            replication: PersistReplicationPolicy::default(),
        }
    }
}

impl From<PersistAppAutoPolicy> for PersistAppPolicy {
    fn from(value: PersistAppAutoPolicy) -> Self {
        Self {
            snapshot_every_ops: value.snapshot_every_ops.max(1),
            replication: value.replication,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedConflictKind {
    OptimisticLock,
    WriteWrite,
    UniqueConstraint,
}

impl fmt::Display for ManagedConflictKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::OptimisticLock => "optimistic_lock",
            Self::WriteWrite => "write_write",
            Self::UniqueConstraint => "unique_constraint",
        };
        write!(f, "{label}")
    }
}

pub fn classify_managed_conflict(err: &DbError) -> Option<ManagedConflictKind> {
    match err {
        DbError::ExecutionError(message) => {
            let lower = message.to_lowercase();
            if lower.contains("optimistic lock conflict") {
                return Some(ManagedConflictKind::OptimisticLock);
            }
            if lower.contains("write-write conflict detected") {
                return Some(ManagedConflictKind::WriteWrite);
            }
            None
        }
        DbError::ConstraintViolation(message) => {
            let lower = message.to_lowercase();
            if lower.contains("unique constraint violation") {
                return Some(ManagedConflictKind::UniqueConstraint);
            }
            None
        }
        _ => None,
    }
}

fn map_managed_conflict_error(operation: &str, err: DbError) -> DbError {
    let Some(kind) = classify_managed_conflict(&err) else {
        return err;
    };

    let prefix = format!("Conflict({kind}) in managed operation '{operation}'");
    match err {
        DbError::ExecutionError(message) => DbError::ExecutionError(format!("{prefix}: {message}")),
        DbError::ConstraintViolation(message) => {
            DbError::ConstraintViolation(format!("{prefix}: {message}"))
        }
        other => other,
    }
}

#[derive(Clone)]
pub struct PersistApp {
    session: PersistSession,
    root: PathBuf,
    policy: PersistAppPolicy,
}

impl PersistApp {
    pub async fn open_auto(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_auto_with(root, PersistAppAutoPolicy::default()).await
    }

    pub async fn open_auto_with(
        root: impl Into<PathBuf>,
        policy: PersistAppAutoPolicy,
    ) -> Result<Self> {
        Self::open(root, policy.into()).await
    }

    pub async fn open(root: impl Into<PathBuf>, policy: PersistAppPolicy) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to create persist app root '{}': {}",
                root.display(),
                err
            ))
        })?;

        Ok(Self {
            session: PersistSession::new(InMemoryDB::new()),
            root,
            policy,
        })
    }

    pub fn policy(&self) -> &PersistAppPolicy {
        &self.policy
    }

    pub async fn open_vec<V>(&self, name: impl Into<String>) -> Result<ManagedPersistVec<V>>
    where
        V: PersistCollection,
    {
        let name = name.into();
        let snapshot_path = self.snapshot_path_for(&name);
        let mut collection = V::new_collection(name.clone());
        let mut last_snapshot_at = None;

        if fs::try_exists(&snapshot_path).await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to check snapshot path '{}': {}",
                snapshot_path.display(),
                err
            ))
        })? {
            let bytes = fs::read(&snapshot_path).await.map_err(|err| {
                DbError::ExecutionError(format!(
                    "Failed to read snapshot '{}': {}",
                    snapshot_path.display(),
                    err
                ))
            })?;

            if !bytes.is_empty() {
                let snapshot: V::Snapshot = serde_json::from_slice(&bytes).map_err(|err| {
                    DbError::ExecutionError(format!(
                        "Failed to decode snapshot '{}': {}",
                        snapshot_path.display(),
                        err
                    ))
                })?;

                collection
                    .restore_with_policy(
                        snapshot,
                        &self.session,
                        RestoreConflictPolicy::OverwriteExisting,
                    )
                    .await?;
                last_snapshot_at = Some(Utc::now().to_rfc3339());
            }
        }

        Ok(ManagedPersistVec {
            name,
            collection,
            session: self.session.clone(),
            snapshot_path,
            snapshot_every_ops: self.policy.snapshot_every_ops.max(1),
            ops_since_snapshot: 0,
            replication: self.policy.replication.clone(),
            replication_failures: 0,
            last_snapshot_at,
        })
    }

    fn snapshot_path_for(&self, vec_name: &str) -> PathBuf {
        let sanitized = vec_name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();
        self.root.join(format!("{sanitized}.snapshot.json"))
    }
}

#[derive(Debug, Clone)]
pub struct ManagedPersistVecStats {
    pub vec_name: String,
    pub item_count: usize,
    pub snapshot_every_ops: usize,
    pub ops_since_snapshot: usize,
    pub snapshot_path: String,
    pub replication_mode: String,
    pub replication_targets: usize,
    pub replication_failures: u64,
    pub last_snapshot_at: Option<String>,
}

pub struct ManagedPersistVec<V: PersistCollection> {
    name: String,
    collection: V,
    session: PersistSession,
    snapshot_path: PathBuf,
    snapshot_every_ops: usize,
    ops_since_snapshot: usize,
    replication: PersistReplicationPolicy,
    replication_failures: u64,
    last_snapshot_at: Option<String>,
}

impl<V: PersistCollection> ManagedPersistVec<V> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn collection(&self) -> &V {
        &self.collection
    }

    pub fn collection_mut(&mut self) -> &mut V {
        &mut self.collection
    }

    pub fn stats(&self) -> ManagedPersistVecStats {
        ManagedPersistVecStats {
            vec_name: self.name.clone(),
            item_count: self.collection.len(),
            snapshot_every_ops: self.snapshot_every_ops,
            ops_since_snapshot: self.ops_since_snapshot,
            snapshot_path: self.snapshot_path.to_string_lossy().to_string(),
            replication_mode: match self.replication.mode {
                PersistReplicationMode::Sync => "sync".to_string(),
                PersistReplicationMode::AsyncBestEffort => "async".to_string(),
            },
            replication_targets: self.replication.replica_roots.len(),
            replication_failures: self.replication_failures,
            last_snapshot_at: self.last_snapshot_at.clone(),
        }
    }

    pub async fn save(&mut self) -> Result<()> {
        self.collection.save_all(&self.session).await?;
        self.on_mutation_committed().await
    }

    pub async fn mutate<F>(&mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut V) -> Result<()>,
    {
        f(&mut self.collection)?;
        self.save().await
    }

    pub async fn mutate_async<F>(&mut self, f: F) -> Result<()>
    where
        F: for<'a> FnOnce(
            &'a mut V,
            &'a PersistSession,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>,
    {
        f(&mut self.collection, &self.session).await?;
        self.save().await
    }

    pub async fn force_snapshot(&mut self) -> Result<()> {
        let snapshot = self.collection.snapshot(SnapshotMode::WithData);
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to encode snapshot for vec '{}': {}",
                self.name, err
            ))
        })?;
        atomic_write(&self.snapshot_path, &bytes).await?;
        self.replicate_snapshot(&bytes).await?;
        self.ops_since_snapshot = 0;
        self.last_snapshot_at = Some(Utc::now().to_rfc3339());
        Ok(())
    }

    async fn on_mutation_committed(&mut self) -> Result<()> {
        self.ops_since_snapshot += 1;
        if self.ops_since_snapshot >= self.snapshot_every_ops {
            self.force_snapshot().await?;
        }
        Ok(())
    }

    async fn begin_atomic_scope(&mut self) -> Result<(V::Snapshot, TransactionId, PersistSession)> {
        let rollback_snapshot = self.collection.snapshot(SnapshotMode::WithData);
        let transaction_id = self.session.begin_transaction().await?;
        let tx_session = self.session.with_transaction_id(transaction_id);
        Ok((rollback_snapshot, transaction_id, tx_session))
    }

    async fn finalize_atomic_scope<T>(
        &mut self,
        operation: &str,
        rollback_snapshot: V::Snapshot,
        transaction_id: TransactionId,
        operation_result: Result<T>,
    ) -> Result<T> {
        match operation_result {
            Ok(value) => {
                if let Err(commit_err) = self.session.commit_transaction(transaction_id).await {
                    let _ = self.session.rollback_transaction(transaction_id).await;
                    let rewind_session = PersistSession::new(InMemoryDB::new());
                    let rewind_result = self
                        .collection
                        .restore_with_policy(
                            rollback_snapshot,
                            &rewind_session,
                            RestoreConflictPolicy::FailFast,
                        )
                        .await;
                    return match rewind_result {
                        Ok(_) => Err(map_managed_conflict_error(operation, commit_err)),
                        Err(rewind_err) => Err(DbError::ExecutionError(format!(
                            "Managed operation '{}' failed to commit and failed to rewind state: commit_error='{}'; rewind_error='{}'",
                            operation, commit_err, rewind_err
                        ))),
                    };
                }
                Ok(value)
            }
            Err(operation_err) => {
                let rollback_result = self.session.rollback_transaction(transaction_id).await;
                let rewind_session = PersistSession::new(InMemoryDB::new());
                let rewind_result = self
                    .collection
                    .restore_with_policy(
                        rollback_snapshot,
                        &rewind_session,
                        RestoreConflictPolicy::FailFast,
                    )
                    .await;

                if let Err(rewind_err) = rewind_result {
                    return Err(DbError::ExecutionError(format!(
                        "Managed operation '{}' failed and rollback state rewind failed: operation_error='{}'; rewind_error='{}'",
                        operation, operation_err, rewind_err
                    )));
                }
                if let Err(rollback_err) = rollback_result {
                    return Err(DbError::ExecutionError(format!(
                        "Managed operation '{}' failed and transaction rollback failed: operation_error='{}'; rollback_error='{}'",
                        operation, operation_err, rollback_err
                    )));
                }

                Err(map_managed_conflict_error(operation, operation_err))
            }
        }
    }

    async fn replicate_snapshot(&mut self, bytes: &[u8]) -> Result<()> {
        if self.replication.replica_roots.is_empty() {
            return Ok(());
        }

        let mode = self.replication.mode.clone();
        let mut failures = 0u64;

        for root in self.replication.replica_roots.clone() {
            let target = root.join(
                self.snapshot_path
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("snapshot.json")),
            );
            if let Err(err) = atomic_write(&target, bytes).await {
                failures += 1;
                if matches!(mode, PersistReplicationMode::Sync) {
                    self.replication_failures += failures;
                    return Err(err);
                }
                warn!(
                    "async snapshot replication failed: vec='{}' replica='{}' error='{}'",
                    self.name,
                    root.display(),
                    err
                );
            }
        }

        self.replication_failures += failures;
        Ok(())
    }
}

impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
{
    pub fn list(&self) -> &[V::Item] {
        self.collection.items()
    }

    pub fn get(&self, persist_id: &str) -> Option<&V::Item> {
        self.collection
            .items()
            .iter()
            .find(|item| item.persist_id() == persist_id && item.metadata().persisted)
    }

    pub fn list_page(&self, offset: usize, limit: usize) -> Vec<&V::Item> {
        if limit == 0 {
            return Vec::new();
        }

        self.collection
            .items()
            .iter()
            .filter(|item| item.metadata().persisted)
            .skip(offset)
            .take(limit)
            .collect()
    }

    pub fn list_filtered<F>(&self, predicate: F) -> Vec<&V::Item>
    where
        F: Fn(&V::Item) -> bool,
    {
        self.collection
            .items()
            .iter()
            .filter(|item| item.metadata().persisted)
            .filter(|item| predicate(item))
            .collect()
    }

    pub fn list_sorted_by<F>(&self, mut compare: F) -> Vec<&V::Item>
    where
        F: FnMut(&V::Item, &V::Item) -> Ordering,
    {
        let mut items = self
            .collection
            .items()
            .iter()
            .filter(|item| item.metadata().persisted)
            .collect::<Vec<_>>();
        items.sort_by(|left, right| compare(left, right));
        items
    }

    pub async fn create(&mut self, item: V::Item) -> Result<()> {
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;
        self.collection.add_one(item);
        let operation_result = self.collection.save_all(&tx_session).await;
        self.finalize_atomic_scope(
            "create",
            rollback_snapshot,
            transaction_id,
            operation_result,
        )
        .await?;
        self.on_mutation_committed().await
    }

    pub async fn create_many(&mut self, items: Vec<V::Item>) -> Result<usize> {
        let count = items.len();
        if count == 0 {
            return Ok(0);
        }

        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;
        self.collection.add_many(items);
        let operation_result = self.collection.save_all(&tx_session).await;
        self.finalize_atomic_scope(
            "create_many",
            rollback_snapshot,
            transaction_id,
            operation_result,
        )
        .await?;
        self.on_mutation_committed().await?;
        Ok(count)
    }

    pub async fn update<F>(&mut self, persist_id: &str, mutator: F) -> Result<bool>
    where
        F: FnOnce(&mut V::Item) -> Result<()>,
    {
        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let operation_result = match self
            .collection
            .items_mut()
            .iter_mut()
            .find(|item| item.persist_id() == persist_id && item.metadata().persisted)
        {
            Some(item) => match mutator(item) {
                Ok(()) => self.collection.save_all(&tx_session).await.map(|_| true),
                Err(err) => Err(err),
            },
            None => Ok(false),
        };

        let updated = self
            .finalize_atomic_scope(
                "update",
                rollback_snapshot,
                transaction_id,
                operation_result,
            )
            .await?;

        if updated {
            self.on_mutation_committed().await?;
        }
        Ok(updated)
    }

    pub async fn apply_many<F>(&mut self, persist_ids: &[String], mutator: F) -> Result<usize>
    where
        F: Fn(&mut V::Item) -> Result<()>,
    {
        let persist_ids = persist_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let mut updated = 0usize;
        let mut operation_result = Ok(());

        for item in self.collection.items_mut().iter_mut() {
            if !item.metadata().persisted {
                continue;
            }
            if !persist_ids.contains(item.persist_id()) {
                continue;
            }

            if let Err(err) = mutator(item) {
                operation_result = Err(err);
                break;
            }
            updated += 1;
        }

        if operation_result.is_ok() && updated > 0 {
            operation_result = self.collection.save_all(&tx_session).await;
        }

        let updated = self
            .finalize_atomic_scope(
                "apply_many",
                rollback_snapshot,
                transaction_id,
                operation_result.map(|_| updated),
            )
            .await?;

        if updated > 0 {
            self.on_mutation_committed().await?;
        }
        Ok(updated)
    }

    pub async fn delete(&mut self, persist_id: &str) -> Result<bool> {
        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let operation_result = match self.collection.remove_by_persist_id(&persist_id) {
            Some(mut item) => item.delete(&tx_session).await.map(|_| true),
            None => Ok(false),
        };

        let deleted = self
            .finalize_atomic_scope(
                "delete",
                rollback_snapshot,
                transaction_id,
                operation_result,
            )
            .await?;

        if deleted {
            self.on_mutation_committed().await?;
        }
        Ok(deleted)
    }

    pub async fn delete_many(&mut self, persist_ids: &[String]) -> Result<usize> {
        let persist_ids = persist_ids.to_vec();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let mut removed = 0usize;
        let mut operation_result = Ok(());
        for persist_id in &persist_ids {
            let mut item = match self.collection.remove_by_persist_id(persist_id) {
                Some(item) => item,
                None => continue,
            };

            if let Err(err) = item.delete(&tx_session).await {
                operation_result = Err(err);
                break;
            }
            removed += 1;
        }

        let removed = self
            .finalize_atomic_scope(
                "delete_many",
                rollback_snapshot,
                transaction_id,
                operation_result.map(|_| removed),
            )
            .await?;

        if removed > 0 {
            self.on_mutation_committed().await?;
        }
        Ok(removed)
    }
}

impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel,
{
    pub fn patch_contract(&self) -> Vec<PersistPatchContract> {
        <V::Item as PersistCommandModel>::patch_contract()
    }

    pub fn command_contract(&self) -> Vec<PersistCommandContract> {
        <V::Item as PersistCommandModel>::command_contract()
    }

    pub async fn create_from_draft(
        &mut self,
        draft: <V::Item as PersistCommandModel>::Draft,
    ) -> Result<String> {
        <V::Item as PersistCommandModel>::validate_draft_payload(&draft)?;
        let item = <V::Item as PersistCommandModel>::try_from_draft(draft)?;
        let persist_id = item.persist_id().to_string();
        self.create(item).await?;
        Ok(persist_id)
    }

    pub async fn patch(
        &mut self,
        persist_id: &str,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<bool> {
        <V::Item as PersistCommandModel>::validate_patch_payload(&patch)?;

        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let operation_result = match self
            .collection
            .items_mut()
            .iter_mut()
            .find(|item| item.persist_id() == persist_id && item.metadata().persisted)
        {
            Some(item) => {
                let changed = <V::Item as PersistCommandModel>::apply_patch_model(item, patch)?;
                if changed {
                    self.collection.save_all(&tx_session).await?;
                }
                Ok((true, changed))
            }
            None => Ok((false, false)),
        };

        let (found, changed) = self
            .finalize_atomic_scope("patch", rollback_snapshot, transaction_id, operation_result)
            .await?;

        if changed {
            self.on_mutation_committed().await?;
        }

        Ok(found)
    }

    pub async fn apply_command(
        &mut self,
        persist_id: &str,
        command: <V::Item as PersistCommandModel>::Command,
    ) -> Result<bool> {
        <V::Item as PersistCommandModel>::validate_command_payload(&command)?;

        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let operation_result = match self
            .collection
            .items_mut()
            .iter_mut()
            .find(|item| item.persist_id() == persist_id && item.metadata().persisted)
        {
            Some(item) => {
                let changed = <V::Item as PersistCommandModel>::apply_command_model(item, command)?;
                if changed {
                    self.collection.save_all(&tx_session).await?;
                }
                Ok((true, changed))
            }
            None => Ok((false, false)),
        };

        let (found, changed) = self
            .finalize_atomic_scope(
                "apply_command",
                rollback_snapshot,
                transaction_id,
                operation_result,
            )
            .await?;

        if changed {
            self.on_mutation_committed().await?;
        }

        Ok(found)
    }
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to create parent directory '{}': {}",
                parent.display(),
                err
            ))
        })?;
    }

    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to write temp file '{}': {}",
            tmp.display(),
            err
        ))
    })?;

    fs::rename(&tmp, path).await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to rename temp file '{}' -> '{}': {}",
            tmp.display(),
            path.display(),
            err
        ))
    })?;
    Ok(())
}
