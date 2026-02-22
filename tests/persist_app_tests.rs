use rustmemodb::{
    DbError, PERSIST_PUBLIC_API_VERSION_MAJOR, PERSIST_PUBLIC_API_VERSION_MINOR,
    PERSIST_PUBLIC_API_VERSION_PATCH, PERSIST_PUBLIC_API_VERSION_STRING, PersistApp,
    PersistAppPolicy, PersistAuditRecordVec, PersistAutonomousIntent, PersistConflictRetryPolicy,
    PersistDomainError, PersistDomainMutationError, PersistEntity, PersistEntityFactory,
    PersistReplicationMode, PersistReplicationPolicy, PersistWorkflowCommandModel,
    RestoreConflictPolicy, SnapshotMode, Value, persist_public_api_version, persist_struct,
    persist_vec,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

persist_struct! {
    pub struct AppTodo {
        title: String,
        done: bool,
    }
}

persist_vec!(pub AppTodoVec, AppTodo);

persist_struct! {
    pub struct AppUser {
        #[persist(unique)]
        email: String,
        display_name: String,
    }
}

persist_vec!(pub AppUserVec, AppUser);

persist_struct! {
    pub struct AppMetric {
        #[persist(index)]
        category: String,
        value: i64,
    }
}

persist_vec!(pub AppMetricVec, AppMetric);

#[derive(Clone, Copy)]
struct AppUserWorkflowCommand {
    display_name: &'static str,
    metric_category: &'static str,
}

impl PersistWorkflowCommandModel<AppUserWorkflowCommand, AppMetric> for AppUser {
    fn to_persist_command(command: &AppUserWorkflowCommand) -> Self::Command {
        AppUserCommand::SetDisplayName(command.display_name.to_string())
    }

    fn to_related_record(
        command: &AppUserWorkflowCommand,
        updated: &Self,
    ) -> rustmemodb::Result<AppMetric> {
        Ok(AppMetric::new(
            format!("{}:{}", command.metric_category, updated.persist_id()),
            updated.metadata().version,
        ))
    }
}

#[derive(Clone, Copy, PersistAutonomousIntent)]
#[persist_intent(model = AppUser, to_command = to_user_command)]
enum AppUserRenameIntent {
    Rename(&'static str),
}

#[derive(Clone, Copy, PersistAutonomousIntent)]
#[persist_intent(model = AppUser)]
enum AppUserFixedRenameIntent {
    #[persist_case(
        command = AppUserCommand::SetDisplayName("Fixed".to_string()),
        event_type = "rename_fixed",
        event_message = "renamed to fixed",
        bulk_event_type = "bulk_rename_fixed",
        bulk_event_message = "bulk renamed to fixed"
    )]
    Fixed,
}

impl AppUserRenameIntent {
    fn to_user_command(self) -> AppUserCommand {
        match self {
            Self::Rename(next_display_name) => {
                AppUserCommand::SetDisplayName(next_display_name.to_string())
            }
        }
    }
}

persist_struct! {
    pub struct DynamicAppTodo from_ddl = "CREATE TABLE dynamic_app_todo (title TEXT NOT NULL, done BOOLEAN)"
}

persist_vec!(pub DynamicAppTodoVec, DynamicAppTodo);

#[test]
fn persist_public_api_version_contract_is_stable_and_consistent() {
    let version = persist_public_api_version();
    assert_eq!(version.major, PERSIST_PUBLIC_API_VERSION_MAJOR);
    assert_eq!(version.minor, PERSIST_PUBLIC_API_VERSION_MINOR);
    assert_eq!(version.patch, PERSIST_PUBLIC_API_VERSION_PATCH);
    assert_eq!(
        PERSIST_PUBLIC_API_VERSION_STRING,
        format!(
            "{}.{}.{}",
            PERSIST_PUBLIC_API_VERSION_MAJOR,
            PERSIST_PUBLIC_API_VERSION_MINOR,
            PERSIST_PUBLIC_API_VERSION_PATCH
        )
    );
    assert!(version.major >= 1, "public API major must be set");
}

#[tokio::test]
async fn persist_app_open_vec_mutate_and_recover_from_snapshot() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_primary");

    let policy = PersistAppPolicy {
        snapshot_every_ops: 1,
        replication: PersistReplicationPolicy::default(),
        conflict_retry: PersistConflictRetryPolicy::default(),
    };

    let app = PersistApp::open(root.clone(), policy.clone())
        .await
        .expect("open app #1");
    let mut todos = app
        .open_vec::<AppTodoVec>("todo_app")
        .await
        .expect("open vec #1");

    todos
        .mutate(|vec| {
            vec.add_one(AppTodo::new("Write tests".to_string(), false));
            Ok(())
        })
        .await
        .expect("mutate and save");

    assert_eq!(todos.collection().items().len(), 1);
    assert_eq!(todos.collection().items()[0].title(), "Write tests");

    let app_restarted = PersistApp::open(root, policy).await.expect("open app #2");
    let restored = app_restarted
        .open_vec::<AppTodoVec>("todo_app")
        .await
        .expect("open vec #2");

    assert_eq!(restored.collection().items().len(), 1);
    assert_eq!(restored.collection().items()[0].title(), "Write tests");
}

#[tokio::test]
async fn persist_app_sync_replication_writes_snapshot_to_replica_root() {
    let temp = tempfile::tempdir().expect("temp dir");
    let primary_root = temp.path().join("primary");
    let replica_root = temp.path().join("replica");

    let policy = PersistAppPolicy {
        snapshot_every_ops: 1,
        replication: PersistReplicationPolicy {
            mode: PersistReplicationMode::Sync,
            replica_roots: vec![replica_root.clone()],
        },
        conflict_retry: PersistConflictRetryPolicy::default(),
    };

    let app = PersistApp::open(primary_root, policy)
        .await
        .expect("open app with replication");
    let mut todos = app
        .open_vec::<AppTodoVec>("todo_app_replica")
        .await
        .expect("open vec");

    todos
        .mutate(|vec| {
            vec.add_one(AppTodo::new("Replicate me".to_string(), false));
            Ok(())
        })
        .await
        .expect("mutate with sync replication");

    let replicated_snapshot = replica_root.join("todo_app_replica.snapshot.json");
    let exists = tokio::fs::try_exists(replicated_snapshot)
        .await
        .expect("check replica snapshot");
    assert!(exists);
}

#[tokio::test]
async fn managed_persist_vec_crud_helpers_work_for_typed_collections() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_crud");

    let app = PersistApp::open(root, PersistAppPolicy::default())
        .await
        .expect("open app");
    let mut todos = app
        .open_vec::<AppTodoVec>("todo_api")
        .await
        .expect("open vec");

    let todo = AppTodo::new("Write CRUD".to_string(), false);
    let todo_id = todo.persist_id().to_string();
    todos.create(todo).await.expect("create one");
    assert_eq!(todos.list().len(), 1);
    assert_eq!(
        todos.get(&todo_id).map(|todo| todo.title().as_str()),
        Some("Write CRUD")
    );

    let updated = todos
        .update(&todo_id, |todo| {
            todo.set_done(true);
            Ok(())
        })
        .await
        .expect("update one");
    assert!(updated);
    assert_eq!(todos.get(&todo_id).map(|todo| *todo.done()), Some(true));

    let first = AppTodo::new("Batch A".to_string(), false);
    let second = AppTodo::new("Batch B".to_string(), false);
    let batch_ids = vec![
        first.persist_id().to_string(),
        second.persist_id().to_string(),
    ];
    let created = todos
        .create_many(vec![first, second])
        .await
        .expect("create many");
    assert_eq!(created, 2);

    let applied = todos
        .apply_many(&batch_ids, |todo| {
            todo.set_done(true);
            Ok(())
        })
        .await
        .expect("apply many");
    assert_eq!(applied, 2);

    for id in &batch_ids {
        assert_eq!(todos.get(id).map(|todo| *todo.done()), Some(true));
    }

    let done_items = todos.list_filtered(|todo| *todo.done());
    assert_eq!(done_items.len(), 3);

    let page = todos.list_page(1, 2);
    assert_eq!(page.len(), 2);

    let sorted = todos.list_sorted_by(|left, right| left.title().cmp(right.title()));
    assert_eq!(sorted.len(), 3);
    assert_eq!(sorted[0].title(), "Batch A");

    let deleted_one = todos.delete(&todo_id).await.expect("delete one");
    assert!(deleted_one);
    assert!(todos.get(&todo_id).is_none());

    let deleted_many = todos.delete_many(&batch_ids).await.expect("delete many");
    assert_eq!(deleted_many, 2);
    assert_eq!(todos.list().len(), 0);
}

#[tokio::test]
async fn persist_app_open_auto_hides_snapshot_lifecycle_from_handlers() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_auto");

    let app = PersistApp::open_auto(root).await.expect("open auto app");
    let mut todos = app
        .open_vec::<AppTodoVec>("todo_auto")
        .await
        .expect("open vec");

    let todo = AppTodo::new("Auto snapshot".to_string(), false);
    todos.create(todo).await.expect("create");

    let stats = todos.stats();
    assert_eq!(stats.snapshot_every_ops, 1);
    assert_eq!(stats.ops_since_snapshot, 0);
    assert!(stats.last_snapshot_at.is_some());
}

#[tokio::test]
async fn persist_app_transaction_commits_and_rolls_back_as_expected() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_transaction");
    let app = PersistApp::open_auto(root).await.expect("open app");

    app.transaction(|tx| async move {
        tx.execute("CREATE TABLE tx_items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            .await?;
        tx.execute("INSERT INTO tx_items (id, value) VALUES (1, 'ok')")
            .await?;
        Ok(())
    })
    .await
    .expect("transaction commit");

    let committed_count = app
        .transaction(|tx| async move {
            let result = tx.query("SELECT COUNT(*) FROM tx_items").await?;
            let count = match result.rows().first().and_then(|row| row.first()) {
                Some(Value::Integer(value)) => *value,
                other => {
                    return Err(DbError::ExecutionError(format!(
                        "unexpected COUNT(*) payload: {other:?}"
                    )));
                }
            };
            Ok(count)
        })
        .await
        .expect("count after commit");
    assert_eq!(committed_count, 1);

    let rollback_err = app
        .transaction(|tx| async move {
            tx.execute("INSERT INTO tx_items (id, value) VALUES (2, 'rollback')")
                .await?;
            Err::<(), DbError>(DbError::ExecutionError("forced rollback".to_string()))
        })
        .await
        .expect_err("transaction must rollback on user error");
    assert!(rollback_err.to_string().contains("forced rollback"));

    let rolled_back_count = app
        .transaction(|tx| async move {
            let result = tx.query("SELECT COUNT(*) FROM tx_items").await?;
            let count = match result.rows().first().and_then(|row| row.first()) {
                Some(Value::Integer(value)) => *value,
                other => {
                    return Err(DbError::ExecutionError(format!(
                        "unexpected COUNT(*) payload: {other:?}"
                    )));
                }
            };
            Ok(count)
        })
        .await
        .expect("count after rollback");
    assert_eq!(rolled_back_count, 1);
}

#[tokio::test]
async fn persist_app_transaction_retries_write_write_conflict_via_policy() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_transaction_retry");

    let app = PersistApp::open(
        root,
        PersistAppPolicy {
            conflict_retry: PersistConflictRetryPolicy {
                max_attempts: 3,
                base_backoff_ms: 1,
                max_backoff_ms: 2,
                retry_write_write: true,
            },
            ..PersistAppPolicy::default()
        },
    )
    .await
    .expect("open app");

    let attempts = Arc::new(AtomicUsize::new(0));
    let call_counter = attempts.clone();
    let applied_attempt = app
        .transaction(move |_tx| {
            let call_counter = call_counter.clone();
            async move {
                let attempt = call_counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    return Err(DbError::ExecutionError(
                        "write-write conflict detected: simulated".to_string(),
                    ));
                }
                Ok::<usize, DbError>(attempt)
            }
        })
        .await
        .expect("transaction should retry and eventually succeed");

    assert_eq!(applied_attempt, 3);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn persist_app_transaction_does_not_retry_optimistic_lock_conflicts() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp
        .path()
        .join("persist_app_transaction_no_optimistic_retry");

    let app = PersistApp::open(
        root,
        PersistAppPolicy {
            conflict_retry: PersistConflictRetryPolicy {
                max_attempts: 5,
                base_backoff_ms: 1,
                max_backoff_ms: 4,
                retry_write_write: true,
            },
            ..PersistAppPolicy::default()
        },
    )
    .await
    .expect("open app");

    let attempts = Arc::new(AtomicUsize::new(0));
    let call_counter = attempts.clone();
    let err = app
        .transaction(move |_tx| {
            let call_counter = call_counter.clone();
            async move {
                let _ = call_counter.fetch_add(1, Ordering::SeqCst) + 1;
                Err::<(), DbError>(DbError::ExecutionError(
                    "optimistic lock conflict: simulated".to_string(),
                ))
            }
        })
        .await
        .expect_err("optimistic lock conflicts must not be retried by policy");

    assert!(
        err.to_string()
            .to_lowercase()
            .contains("optimistic lock conflict"),
        "unexpected error: {err}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "optimistic lock is business-level if-match conflict and must not auto-retry"
    );
}

#[tokio::test]
async fn managed_atomic_with_commits_changes_across_two_collections() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_atomic_with_commit");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_atomic_commit")
        .await
        .expect("open users vec");
    let mut metrics = app
        .open_vec::<AppMetricVec>("metrics_atomic_commit")
        .await
        .expect("open metrics vec");

    let user = AppUser::new("atomic@example.com".to_string(), "Atomic".to_string());
    let user_id = user.persist_id().to_string();

    users
        .atomic_with(&mut metrics, move |tx, users, metrics| {
            Box::pin(async move {
                users.create_with_tx(&tx, user).await?;
                metrics
                    .create_with_tx(&tx, AppMetric::new("signup".to_string(), 1))
                    .await?;
                Ok::<(), DbError>(())
            })
        })
        .await
        .expect("atomic commit across users and metrics");

    assert!(users.get(&user_id).is_some(), "user must be committed");
    assert_eq!(metrics.list().len(), 1, "metric must be committed");
}

#[tokio::test]
async fn managed_atomic_with_rolls_back_both_collections_on_error() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_atomic_with_rollback");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_atomic_rollback")
        .await
        .expect("open users vec");
    let mut metrics = app
        .open_vec::<AppMetricVec>("metrics_atomic_rollback")
        .await
        .expect("open metrics vec");

    users
        .create(AppUser::new(
            "existing@example.com".to_string(),
            "Existing".to_string(),
        ))
        .await
        .expect("seed existing user");

    let duplicate = AppUser::new("existing@example.com".to_string(), "Duplicate".to_string());

    let err = metrics
        .atomic_with(&mut users, move |tx, metrics, users| {
            Box::pin(async move {
                metrics
                    .create_with_tx(&tx, AppMetric::new("should_rollback".to_string(), 42))
                    .await?;
                users.create_with_tx(&tx, duplicate).await?;
                Ok::<(), DbError>(())
            })
        })
        .await
        .expect_err("duplicate user must rollback both collections");

    let lower = err.to_string().to_lowercase();
    assert!(
        lower.contains("unique constraint"),
        "unexpected atomic rollback error: {err}"
    );
    assert_eq!(users.list().len(), 1, "users must be rewound");
    assert_eq!(metrics.list().len(), 0, "metrics must be rewound");
}

#[tokio::test]
async fn managed_execute_command_if_match_updates_entity_without_manual_version_checks() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_execute_command_if_match");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_execute_if_match")
        .await
        .expect("open users vec");

    let user = AppUser::new("command@example.com".to_string(), "Initial".to_string());
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let updated = users
        .execute_command_if_match(
            &user_id,
            1,
            AppUserCommand::SetDisplayName("Updated".to_string()),
        )
        .await
        .expect("execute command with if-match")
        .expect("user must exist");
    assert_eq!(updated.display_name(), "Updated");
    assert_eq!(updated.metadata().version, 2);

    let stale_result = users
        .execute_command_if_match(
            &user_id,
            1,
            AppUserCommand::SetDisplayName("Stale".to_string()),
        )
        .await;
    assert!(
        stale_result.is_err(),
        "stale version must fail for execute_command_if_match"
    );
    let err = stale_result
        .err()
        .expect("error must be present on stale if-match");
    let lower = err.to_string().to_lowercase();
    assert!(
        lower.contains("optimistic lock conflict"),
        "unexpected stale command error: {err}"
    );
}

#[tokio::test]
async fn managed_execute_patch_if_match_updates_entity_without_manual_version_checks() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_execute_patch_if_match");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_patch_if_match")
        .await
        .expect("open users vec");

    let user = AppUser::new("patch@example.com".to_string(), "Initial".to_string());
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let updated = users
        .execute_patch_if_match(
            &user_id,
            1,
            AppUserPatch {
                display_name: Some("Patched".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("execute patch with if-match")
        .expect("user must exist");
    assert_eq!(updated.display_name(), "Patched");
    assert_eq!(updated.metadata().version, 2);

    let stale_result = users
        .execute_patch_if_match(
            &user_id,
            1,
            AppUserPatch {
                display_name: Some("Stale".to_string()),
                ..Default::default()
            },
        )
        .await;
    assert!(
        stale_result.is_err(),
        "stale version must fail for execute_patch_if_match"
    );
    let err = stale_result
        .err()
        .expect("error must be present on stale patch if-match");
    let lower = err.to_string().to_lowercase();
    assert!(
        lower.contains("optimistic lock conflict"),
        "unexpected stale patch error: {err}"
    );
}

#[tokio::test]
async fn managed_execute_delete_if_match_applies_optimistic_lock_without_manual_checks() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_execute_delete_if_match");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_delete_if_match")
        .await
        .expect("open users vec");

    let user = AppUser::new("delete@example.com".to_string(), "ToDelete".to_string());
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let stale_result = users.execute_delete_if_match(&user_id, 2).await;
    assert!(
        stale_result.is_err(),
        "stale version must fail for execute_delete_if_match"
    );
    let err = stale_result
        .err()
        .expect("error must be present on stale delete if-match");
    let lower = err.to_string().to_lowercase();
    assert!(
        lower.contains("optimistic lock conflict"),
        "unexpected stale delete error: {err}"
    );

    let deleted = users
        .execute_delete_if_match(&user_id, 1)
        .await
        .expect("delete if-match must succeed");
    assert!(deleted);
    assert!(users.get(&user_id).is_none(), "user must be removed");

    let missing = users
        .execute_delete_if_match("missing-user", 1)
        .await
        .expect("missing delete if-match");
    assert!(!missing, "missing user should return false");
}

#[tokio::test]
async fn managed_execute_command_if_match_with_create_appends_related_record_atomically() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp
        .path()
        .join("persist_app_execute_command_if_match_with_create");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_execute_with_create")
        .await
        .expect("open users vec");
    let mut metrics = app
        .open_vec::<AppMetricVec>("metrics_execute_with_create")
        .await
        .expect("open metrics vec");

    let user = AppUser::new("audit@example.com".to_string(), "Initial".to_string());
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let updated = users
        .execute_command_if_match_with_create(
            &mut metrics,
            &user_id,
            1,
            AppUserCommand::SetDisplayName("Updated".to_string()),
            |updated| {
                Ok(AppMetric::new(
                    format!("user_updated:{}", updated.persist_id()),
                    updated.metadata().version,
                ))
            },
        )
        .await
        .expect("execute command + append related")
        .expect("user must exist");
    assert_eq!(updated.display_name(), "Updated");
    assert_eq!(metrics.list().len(), 1, "related record must be committed");

    let stale_result = users
        .execute_command_if_match_with_create(
            &mut metrics,
            &user_id,
            1,
            AppUserCommand::SetDisplayName("Nope".to_string()),
            |_updated| Ok(AppMetric::new("should_not_exist".to_string(), 0)),
        )
        .await;
    assert!(
        stale_result.is_err(),
        "side-effect command with stale version must fail"
    );
    let err = stale_result
        .err()
        .expect("error must be present on stale side-effect command");
    let lower = err.to_string().to_lowercase();
    assert!(
        lower.contains("optimistic lock conflict"),
        "unexpected stale execute_with_create error: {err}"
    );
    assert_eq!(
        metrics.list().len(),
        1,
        "failed command must not append related record"
    );
}

#[tokio::test]
async fn managed_execute_workflow_if_match_with_create_hides_inline_closure_plumbing() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_execute_workflow_with_create");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_execute_workflow")
        .await
        .expect("open users vec");
    let mut metrics = app
        .open_vec::<AppMetricVec>("metrics_execute_workflow")
        .await
        .expect("open metrics vec");

    let user = AppUser::new("workflow@example.com".to_string(), "Initial".to_string());
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let updated = users
        .execute_workflow_if_match_with_create(
            &mut metrics,
            &user_id,
            1,
            AppUserWorkflowCommand {
                display_name: "Workflow Updated",
                metric_category: "user_workflow",
            },
        )
        .await
        .expect("execute workflow with create")
        .expect("user must exist");
    assert_eq!(updated.display_name(), "Workflow Updated");
    assert_eq!(metrics.list().len(), 1);

    let stale_result = users
        .execute_workflow_if_match_with_create(
            &mut metrics,
            &user_id,
            1,
            AppUserWorkflowCommand {
                display_name: "Should conflict",
                metric_category: "user_workflow",
            },
        )
        .await;
    assert!(
        stale_result.is_err(),
        "stale workflow command must fail with optimistic lock"
    );
    let err = stale_result
        .err()
        .expect("error must be present on stale workflow");
    let lower = err.to_string().to_lowercase();
    assert!(
        lower.contains("optimistic lock conflict"),
        "unexpected stale workflow error: {err}"
    );
    assert_eq!(
        metrics.list().len(),
        1,
        "failed workflow must not append extra related records"
    );
}

#[tokio::test]
async fn managed_execute_workflow_for_many_with_create_many_hides_bulk_plumbing() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_execute_workflow_many");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_execute_workflow_many")
        .await
        .expect("open users vec");
    let mut metrics = app
        .open_vec::<AppMetricVec>("metrics_execute_workflow_many")
        .await
        .expect("open metrics vec");

    let first = AppUser::new(
        "workflow-many-1@example.com".to_string(),
        "Initial1".to_string(),
    );
    let first_id = first.persist_id().to_string();
    let second = AppUser::new(
        "workflow-many-2@example.com".to_string(),
        "Initial2".to_string(),
    );
    let second_id = second.persist_id().to_string();
    users
        .create_many(vec![first, second])
        .await
        .expect("seed users");

    let processed = users
        .execute_workflow_for_many_with_create_many(
            &mut metrics,
            &[first_id.clone(), second_id.clone()],
            AppUserWorkflowCommand {
                display_name: "Bulk Workflow Updated",
                metric_category: "user_workflow_many",
            },
        )
        .await
        .expect("execute workflow many");
    assert_eq!(processed, 2);
    assert_eq!(metrics.list().len(), 2);
    assert_eq!(
        users.get(&first_id).map(|u| u.display_name().as_str()),
        Some("Bulk Workflow Updated")
    );
    assert_eq!(
        users.get(&second_id).map(|u| u.display_name().as_str()),
        Some("Bulk Workflow Updated")
    );
}

#[tokio::test]
async fn persist_aggregate_store_exposes_intent_level_helpers_without_managed_vec_plumbing() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_aggregate_store_api");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_aggregate::<AppUserVec>("users_aggregate_store")
        .await
        .expect("open users aggregate");
    let mut metrics = app
        .open_aggregate::<AppMetricVec>("metrics_aggregate_store")
        .await
        .expect("open metrics aggregate");

    let user = AppUser::new(
        "aggregate-store@example.com".to_string(),
        "Initial".to_string(),
    );
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let patched = users
        .execute_patch_if_match(
            &user_id,
            1,
            AppUserPatch {
                display_name: Some("Patched".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("patch user")
        .expect("user must exist");
    assert_eq!(patched.display_name(), "Patched");
    assert_eq!(patched.metadata().version, 2);

    let workflow_updated = users
        .execute_workflow_if_match_with_create(
            &mut metrics,
            &user_id,
            2,
            AppUserWorkflowCommand {
                display_name: "Workflow Updated",
                metric_category: "aggregate_store_workflow",
            },
        )
        .await
        .expect("workflow update")
        .expect("user should exist");
    assert_eq!(workflow_updated.display_name(), "Workflow Updated");
    assert_eq!(workflow_updated.metadata().version, 3);
    assert_eq!(metrics.list().len(), 1);

    let deleted = users
        .execute_delete_if_match(&user_id, 3)
        .await
        .expect("delete by if-match");
    assert!(deleted);
    assert!(users.get(&user_id).is_none());
}

#[tokio::test]
async fn persist_aggregate_store_query_page_filtered_sorted_handles_filter_sort_and_metadata() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_aggregate_query_page");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut todos = app
        .open_aggregate::<AppTodoVec>("todos_aggregate_query_page")
        .await
        .expect("open todos aggregate");

    todos
        .create_many(vec![
            AppTodo::new("Gamma".to_string(), true),
            AppTodo::new("Alpha".to_string(), true),
            AppTodo::new("Beta".to_string(), true),
            AppTodo::new("Zeta".to_string(), false),
        ])
        .await
        .expect("seed todos");

    let page = todos.query_page_filtered_sorted(
        1,
        2,
        |todo| *todo.done(),
        |left, right| left.title().cmp(right.title()),
    );

    assert_eq!(page.total, 3);
    assert_eq!(page.total_pages, 2);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].title(), "Alpha");
    assert_eq!(page.items[1].title(), "Beta");

    let second_page = todos.query_page_filtered_sorted(
        2,
        2,
        |todo| *todo.done(),
        |left, right| left.title().cmp(right.title()),
    );
    assert_eq!(second_page.total, 3);
    assert_eq!(second_page.total_pages, 2);
    assert_eq!(second_page.items.len(), 1);
    assert_eq!(second_page.items[0].title(), "Gamma");
}

#[tokio::test]
async fn persist_aggregate_store_auto_audit_helpers_append_records_without_manual_workflow_types() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_aggregate_auto_audit");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_aggregate::<AppUserVec>("users_aggregate_auto_audit")
        .await
        .expect("open users aggregate");
    let mut audits = app
        .open_aggregate::<PersistAuditRecordVec>("audits_aggregate_auto_audit")
        .await
        .expect("open audits aggregate");

    let first = AppUser::new(
        "audit-helper-1@example.com".to_string(),
        "Initial1".to_string(),
    );
    let first_id = first.persist_id().to_string();
    let second = AppUser::new(
        "audit-helper-2@example.com".to_string(),
        "Initial2".to_string(),
    );
    let second_id = second.persist_id().to_string();
    users
        .create_many(vec![first, second])
        .await
        .expect("seed users");

    let updated = users
        .execute_intent_if_match_auto_audit(
            &mut audits,
            &first_id,
            1,
            "rename_intent",
            |_intent| AppUserCommand::SetDisplayName("Renamed".to_string()),
            |_intent| "rename",
            |_intent| "renamed once",
        )
        .await
        .expect("single command + audit")
        .expect("first user should exist");
    assert_eq!(updated.display_name(), "Renamed");
    assert_eq!(updated.metadata().version, 2);

    let processed = users
        .execute_intent_for_many_auto_audit(
            &mut audits,
            &[first_id.clone(), second_id.clone()],
            "bulk_rename_intent",
            |_intent| AppUserCommand::SetDisplayName("Bulk Renamed".to_string()),
            |_intent| "bulk_rename",
            |_intent| "renamed in bulk",
        )
        .await
        .expect("bulk command + audit");
    assert_eq!(processed, 2);

    let first_audits = audits.list_filtered(|record| record.aggregate_persist_id() == &first_id);
    assert_eq!(first_audits.len(), 2);
    assert!(
        first_audits
            .iter()
            .any(|record| record.event_type() == "rename"),
        "first user must have single-event audit record"
    );
    assert!(
        first_audits
            .iter()
            .any(|record| record.event_type() == "bulk_rename"),
        "first user must have bulk-event audit record"
    );

    let second_audits = audits.list_filtered(|record| record.aggregate_persist_id() == &second_id);
    assert_eq!(second_audits.len(), 1);
    assert_eq!(second_audits[0].event_type(), "bulk_rename");
}

#[tokio::test]
async fn persist_autonomous_aggregate_applies_commands_without_mapper_boilerplate() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_autonomous_aggregate");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_autonomous::<AppUserVec>("users_autonomous")
        .await
        .expect("open autonomous users aggregate");

    let first = AppUser::new(
        "autonomous-1@example.com".to_string(),
        "Initial1".to_string(),
    );
    let first_id = first.persist_id().to_string();
    let second = AppUser::new(
        "autonomous-2@example.com".to_string(),
        "Initial2".to_string(),
    );
    let second_id = second.persist_id().to_string();

    users
        .create_many(vec![first, second])
        .await
        .expect("seed users");

    let updated = users
        .apply(&first_id, 1, AppUserRenameIntent::Rename("Renamed"))
        .await
        .expect("single command apply")
        .expect("first user should exist");
    assert_eq!(updated.display_name(), "Renamed");
    assert_eq!(updated.metadata().version, 2);

    let processed = users
        .apply_many(
            &[first_id.clone(), second_id.clone()],
            AppUserRenameIntent::Rename("Bulk Renamed"),
        )
        .await
        .expect("bulk command apply");
    assert_eq!(processed, 2);

    assert_eq!(
        users.get(&first_id).map(|u| u.display_name().as_str()),
        Some("Bulk Renamed")
    );
    assert_eq!(
        users.get(&second_id).map(|u| u.display_name().as_str()),
        Some("Bulk Renamed")
    );

    let first_audits = users.list_audits_for(&first_id);
    assert_eq!(first_audits.len(), 2);
    assert!(
        first_audits
            .iter()
            .any(|record| record.event_type() == "set_display_name"),
        "first user must have single system-event audit record"
    );
    assert!(
        first_audits
            .iter()
            .any(|record| record.event_type() == "bulk_set_display_name"),
        "first user must have bulk system-event audit record"
    );

    let second_audits = users.list_audits_for(&second_id);
    assert_eq!(second_audits.len(), 1);
    assert_eq!(second_audits[0].event_type(), "bulk_set_display_name");
}

#[tokio::test]
async fn persist_autonomous_intent_supports_variant_mapping_without_impl_block() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_autonomous_variant_mapping");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_autonomous::<AppUserVec>("users_autonomous_variant")
        .await
        .expect("open autonomous users aggregate");

    let user = AppUser::new(
        "autonomous-variant@example.com".to_string(),
        "Initial".to_string(),
    );
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let updated = users
        .apply(&user_id, 1, AppUserFixedRenameIntent::Fixed)
        .await
        .expect("apply command")
        .expect("user should exist");
    assert_eq!(updated.display_name(), "Fixed");
    assert_eq!(updated.metadata().version, 2);

    let audits = users.list_audits_for(&user_id);
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].event_type(), "rename_fixed");
    assert_eq!(audits[0].message(), "renamed to fixed");
}

#[tokio::test]
async fn persist_domain_store_hides_expected_version_for_quick_start_api() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_domain_store");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_domain::<AppUserVec>("users_domain")
        .await
        .expect("open domain users store");

    let user = AppUser::new("domain@example.com".to_string(), "Initial".to_string());
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let renamed = users
        .intent(&user_id, AppUserRenameIntent::Rename("Renamed"))
        .await
        .expect("intent should succeed")
        .expect("user should exist");
    assert_eq!(renamed.display_name(), "Renamed");
    assert_eq!(renamed.metadata().version, 2);

    let patched = users
        .patch(
            &user_id,
            AppUserPatch {
                display_name: Some("Patched".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("patch should succeed")
        .expect("patched user should exist");
    assert_eq!(patched.display_name(), "Patched");
    assert_eq!(patched.metadata().version, 3);

    let deleted = users.remove(&user_id).await.expect("delete should succeed");
    assert!(deleted);
    assert!(users.get(&user_id).is_none(), "user should be deleted");

    let audits = users.list_audits_for(&user_id);
    assert_eq!(audits.len(), 1, "intent call should auto-write audit event");
    assert_eq!(audits[0].event_type(), "set_display_name");
}

#[tokio::test]
async fn persist_domain_store_outcome_api_returns_domain_errors_without_db_leaks() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_domain_store_outcome_api");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_domain::<AppUserVec>("users_domain_outcome")
        .await
        .expect("open domain users store");

    let created = users
        .create_one(AppUser::new(
            "outcome@example.com".to_string(),
            "Initial".to_string(),
        ))
        .await
        .expect("create_one should succeed");
    let user_id = created.persist_id().to_string();

    let renamed = users
        .intent_one(&user_id, AppUserRenameIntent::Rename("Renamed"))
        .await
        .expect("intent_one should succeed");
    assert_eq!(renamed.display_name(), "Renamed");

    let patched = users
        .patch_one(
            &user_id,
            AppUserPatch {
                display_name: Some("Patched".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("patch_one should succeed");
    assert_eq!(patched.display_name(), "Patched");

    let duplicate = match users
        .create_one(AppUser::new(
            "outcome@example.com".to_string(),
            "Duplicate".to_string(),
        ))
        .await
    {
        Ok(_) => panic!("duplicate email should fail"),
        Err(err) => err,
    };
    assert!(matches!(duplicate, PersistDomainError::ConflictUnique(_)));

    let missing_id = "00000000-0000-0000-0000-000000000000";
    let missing_patch = match users
        .patch_one(
            missing_id,
            AppUserPatch {
                display_name: Some("Missing".to_string()),
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => panic!("missing aggregate should map to not_found"),
        Err(err) => err,
    };
    assert_eq!(missing_patch, PersistDomainError::NotFound);

    users
        .remove_one(&user_id)
        .await
        .expect("remove_one should delete existing user");

    let missing_remove = users
        .remove_one(&user_id)
        .await
        .expect_err("second remove should report not_found");
    assert_eq!(missing_remove, PersistDomainError::NotFound);
}

#[tokio::test]
async fn persist_domain_handle_hides_locking_and_supports_outcome_api() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_domain_handle_outcome_api");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let users = app
        .open_domain_handle::<AppUserVec>("users_domain_handle")
        .await
        .expect("open domain users handle");

    let created = users
        .create_one(AppUser::new(
            "handle@example.com".to_string(),
            "Initial".to_string(),
        ))
        .await
        .expect("create_one should succeed");
    let user_id = created.persist_id().to_string();

    let renamed = users
        .intent_one(&user_id, AppUserRenameIntent::Rename("Renamed"))
        .await
        .expect("intent_one should succeed");
    assert_eq!(renamed.display_name(), "Renamed");

    let mutated = users
        .mutate_one(&user_id, |user| {
            user.set_display_name("Mutated".to_string());
            Ok(())
        })
        .await
        .expect("mutate_one should succeed");
    assert_eq!(mutated.display_name(), "Mutated");

    let listed = users.list().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].persist_id(), user_id);

    let fetched = users
        .get_one(&user_id)
        .await
        .expect("entity should be fetchable");
    assert_eq!(fetched.display_name(), "Mutated");

    users
        .remove_one(&user_id)
        .await
        .expect("remove_one should delete existing user");
    let missing_remove = users
        .remove_one(&user_id)
        .await
        .expect_err("second remove should report not_found");
    assert_eq!(missing_remove, PersistDomainError::NotFound);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserMutatorError {
    EmptyDisplayName,
}

impl std::fmt::Display for UserMutatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyDisplayName => write!(f, "display_name must not be empty"),
        }
    }
}

impl std::error::Error for UserMutatorError {}

#[tokio::test]
async fn persist_domain_handle_mutate_one_with_preserves_user_error_and_rolls_back() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_domain_handle_mutate_with");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let users = app
        .open_domain_handle::<AppUserVec>("users_domain_handle_mutate_with")
        .await
        .expect("open domain users handle");

    let created = users
        .create_one(AppUser::new(
            "mutate-with@example.com".to_string(),
            "Initial".to_string(),
        ))
        .await
        .expect("create_one should succeed");
    let user_id = created.persist_id().to_string();

    let user_error = match users
        .mutate_one_with(&user_id, |_user| Err(UserMutatorError::EmptyDisplayName))
        .await
    {
        Ok(_) => panic!("mutate_one_with should return user error"),
        Err(error) => error,
    };
    assert_eq!(
        user_error,
        PersistDomainMutationError::User(UserMutatorError::EmptyDisplayName)
    );

    let unchanged = users
        .get_one(&user_id)
        .await
        .expect("entity should still exist after rollback");
    assert_eq!(unchanged.display_name(), "Initial");
}

#[tokio::test]
async fn persist_domain_store_supports_workflow_without_manual_expected_version() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_domain_store_workflow");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_domain::<AppUserVec>("users_domain_workflow")
        .await
        .expect("open users domain store");
    let mut metrics = app
        .open_domain::<AppMetricVec>("metrics_domain_workflow")
        .await
        .expect("open metrics domain store");

    let user = AppUser::new(
        "domain-workflow@example.com".to_string(),
        "Initial".to_string(),
    );
    let user_id = user.persist_id().to_string();
    users.create(user).await.expect("seed user");

    let updated = users
        .workflow_with_create(
            &mut metrics,
            &user_id,
            AppUserWorkflowCommand {
                display_name: "Workflow Updated",
                metric_category: "domain_workflow",
            },
        )
        .await
        .expect("workflow should succeed")
        .expect("user should exist");
    assert_eq!(updated.display_name(), "Workflow Updated");
    assert_eq!(updated.metadata().version, 2);
    assert_eq!(metrics.list().len(), 1);
}

#[tokio::test]
async fn persist_app_legacy_adapter_supports_old_vector_style_flow() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_legacy_adapter");

    let app = PersistApp::open_auto(root.clone())
        .await
        .expect("open auto app");
    let mut legacy = app
        .open_vec_legacy::<AppTodoVec>("todo_legacy")
        .await
        .expect("open legacy vec");

    legacy.add_one(AppTodo::new("Legacy one".to_string(), false));
    legacy.add_many(vec![AppTodo::new("Legacy two".to_string(), false)]);
    assert_eq!(legacy.len(), 2);

    let snapshot = legacy.snapshot(SnapshotMode::WithData);
    assert_eq!(snapshot.states.len(), 2);

    legacy.save_all().await.expect("legacy save all");

    let app_restarted = PersistApp::open_auto(root.clone())
        .await
        .expect("open restart app");
    let restored_managed = app_restarted
        .open_vec::<AppTodoVec>("todo_legacy")
        .await
        .expect("open managed vec after restart");
    assert_eq!(restored_managed.list().len(), 2);

    let mut another = app_restarted
        .open_vec_legacy::<AppTodoVec>("todo_legacy_restored")
        .await
        .expect("open second legacy vec");
    another
        .restore_with_policy(snapshot, RestoreConflictPolicy::OverwriteExisting)
        .await
        .expect("restore from snapshot via adapter");
    assert_eq!(another.len(), 2);

    another
        .force_snapshot()
        .await
        .expect("legacy force snapshot");
    let snapshot_path = root.join("todo_legacy_restored.snapshot.json");
    let exists = tokio::fs::try_exists(snapshot_path)
        .await
        .expect("check restored snapshot");
    assert!(exists);
}

#[tokio::test]
async fn managed_command_first_api_works_with_draft_patch_and_command() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_command_first");

    let app = PersistApp::open_auto(root).await.expect("open auto app");
    let mut todos = app
        .open_vec::<AppTodoVec>("todo_command_first")
        .await
        .expect("open vec");

    let draft_id = todos
        .create_from_draft(AppTodoDraft::new("From draft".to_string(), false))
        .await
        .expect("create from draft");

    let found = todos
        .patch(
            &draft_id,
            AppTodoPatch {
                done: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("patch");
    assert!(found);

    let found = todos
        .apply_command(
            &draft_id,
            AppTodoCommand::SetTitle("After command".to_string()),
        )
        .await
        .expect("apply command");
    assert!(found);

    let todo = todos.get(&draft_id).expect("todo by id");
    assert_eq!(todo.title(), "After command");
    assert_eq!(*todo.done(), true);

    let patch_contract = todos.patch_contract();
    assert!(patch_contract.iter().any(|field| field.field == "title"));
    let command_contract = todos.command_contract();
    assert!(command_contract.iter().any(|cmd| cmd.name == "SetDone"));

    let err = todos
        .patch(&draft_id, AppTodoPatch::default())
        .await
        .expect_err("empty patch must fail");
    assert!(err.to_string().contains("Patch payload"));
}

#[tokio::test]
async fn managed_unique_field_constraint_blocks_duplicate_create_and_patch() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_unique_field");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut users = app
        .open_vec::<AppUserVec>("users_unique")
        .await
        .expect("open users vec");

    let unique_fields =
        AppUser::new("probe@example.com".to_string(), "Probe".to_string()).unique_fields();
    assert_eq!(unique_fields, vec!["email"]);
    let indexed_fields =
        AppUser::new("probe@example.com".to_string(), "Probe".to_string()).indexed_fields();
    assert_eq!(indexed_fields, vec!["email"]);

    let first = AppUser::new("alice@example.com".to_string(), "Alice".to_string());
    let second = AppUser::new("bob@example.com".to_string(), "Bob".to_string());
    let second_id = second.persist_id().to_string();

    users
        .create_many(vec![first, second])
        .await
        .expect("seed users");

    let duplicate = AppUser::new("alice@example.com".to_string(), "Alice 2".to_string());
    let err = users
        .create(duplicate)
        .await
        .expect_err("duplicate create must fail");
    let err_text = err.to_string().to_lowercase();
    assert!(
        err_text.contains("unique constraint violation"),
        "unexpected error text: {err}"
    );
    assert_eq!(users.list().len(), 2, "failed create must roll back fully");

    let err = users
        .patch(
            &second_id,
            AppUserPatch {
                email: Some("alice@example.com".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("duplicate patch must fail");
    let err_text = err.to_string().to_lowercase();
    assert!(
        err_text.contains("unique constraint violation"),
        "unexpected error text: {err}"
    );

    let unchanged = users.get(&second_id).expect("second user must remain");
    assert_eq!(unchanged.email(), "bob@example.com");
}

#[tokio::test]
async fn managed_indexed_field_is_declared_and_saves_without_manual_index_sql() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_index_field");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut metrics = app
        .open_vec::<AppMetricVec>("metrics_indexed")
        .await
        .expect("open metrics vec");

    let indexed_fields = AppMetric::new("latency".to_string(), 120).indexed_fields();
    assert_eq!(indexed_fields, vec!["category"]);

    metrics
        .create_many(vec![
            AppMetric::new("latency".to_string(), 120),
            AppMetric::new("latency".to_string(), 80),
        ])
        .await
        .expect("non-unique indexed field should allow duplicates");
    assert_eq!(metrics.list().len(), 2);
}

#[tokio::test]
async fn managed_command_first_api_works_for_dynamic_entities() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_command_first_dynamic");

    let app = PersistApp::open_auto(root).await.expect("open auto app");
    let mut todos = app
        .open_vec::<DynamicAppTodoVec>("todo_command_first_dynamic")
        .await
        .expect("open vec");

    let draft_id = todos
        .create_from_draft(
            DynamicAppTodoDraft::new()
                .with_field("title", Value::Text("Dynamic draft".to_string()))
                .expect("draft title"),
        )
        .await
        .expect("create from draft");

    let found = todos
        .patch(
            &draft_id,
            DynamicAppTodoPatch::new()
                .with_field("done", Value::Boolean(true))
                .expect("patch field"),
        )
        .await
        .expect("patch");
    assert!(found);

    let found = todos
        .apply_command(
            &draft_id,
            DynamicAppTodoCommand::set("title", Value::Text("Dynamic command".to_string())),
        )
        .await
        .expect("apply command");
    assert!(found);

    let todo = todos.get(&draft_id).expect("todo by id");
    assert_eq!(
        todo.get_field("title"),
        Some(&Value::Text("Dynamic command".to_string()))
    );
    assert_eq!(todo.get_field("done"), Some(&Value::Boolean(true)));

    let patch_contract = todos.patch_contract();
    assert!(patch_contract.iter().any(|field| field.field == "title"));
    let command_contract = todos.command_contract();
    assert!(command_contract.iter().any(|cmd| cmd.name == "SetField"));

    let err = todos
        .create_from_draft(DynamicAppTodoDraft::new())
        .await
        .expect_err("missing required title must fail");
    assert!(err.to_string().contains("non-null field 'title'"));
}

#[tokio::test]
async fn managed_create_many_is_atomic_on_conflict() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_atomic_create_many");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut todos = app
        .open_vec::<AppTodoVec>("todo_atomic")
        .await
        .expect("open vec");

    let seed = AppTodo::new("Seed".to_string(), false);
    let seed_id = seed.persist_id().to_string();
    todos.create(seed).await.expect("seed create");

    let mut duplicate_state = todos.get(&seed_id).expect("seed must exist").state();
    duplicate_state.metadata.persisted = false;
    duplicate_state.metadata.version = 0;
    duplicate_state.metadata.touch_count = 0;
    let duplicate = AppTodo::from_state(&duplicate_state).expect("duplicate from state");

    let fresh = AppTodo::new("Fresh should rollback".to_string(), false);
    let err = todos
        .create_many(vec![fresh, duplicate])
        .await
        .expect_err("expected create_many conflict");
    let err_text = err.to_string().to_lowercase();
    assert!(
        err_text.contains("conflict(") && err_text.contains("create_many"),
        "unexpected error text: {err}"
    );

    assert_eq!(todos.list().len(), 1, "atomic rollback must keep only seed");
    assert_eq!(todos.list()[0].title(), "Seed");
}

#[tokio::test]
async fn managed_apply_many_is_atomic_on_mutator_error() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_atomic_apply_many");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut todos = app
        .open_vec::<AppTodoVec>("todo_atomic_apply")
        .await
        .expect("open vec");

    let first = AppTodo::new("First".to_string(), false);
    let second = AppTodo::new("Second".to_string(), false);
    let ids = vec![
        first.persist_id().to_string(),
        second.persist_id().to_string(),
    ];
    todos
        .create_many(vec![first, second])
        .await
        .expect("seed many");

    let err = todos
        .apply_many(&ids, |todo| {
            if todo.title() == "Second" {
                return Err(DbError::ExecutionError(
                    "intentional mutator failure".to_string(),
                ));
            }
            todo.set_done(true);
            Ok(())
        })
        .await
        .expect_err("apply_many should rollback entire batch");
    assert!(
        err.to_string().contains("intentional mutator failure"),
        "unexpected error: {err}"
    );

    for id in &ids {
        assert_eq!(
            todos.get(id).map(|todo| *todo.done()),
            Some(false),
            "no partial updates allowed when apply_many fails"
        );
    }
}

#[tokio::test]
async fn managed_update_exposes_explicit_optimistic_conflict_for_stale_collection() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_conflict_update");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let mut primary = app
        .open_vec::<AppTodoVec>("todo_conflict")
        .await
        .expect("open primary");

    let todo = AppTodo::new("Conflict item".to_string(), false);
    let todo_id = todo.persist_id().to_string();
    primary.create(todo).await.expect("create");

    let mut stale = app
        .open_vec::<AppTodoVec>("todo_conflict")
        .await
        .expect("open stale");
    assert_eq!(stale.get(&todo_id).map(|todo| *todo.done()), Some(false));

    let changed = primary
        .update(&todo_id, |todo| {
            todo.set_done(true);
            Ok(())
        })
        .await
        .expect("primary update");
    assert!(changed);

    let err = stale
        .update(&todo_id, |todo| {
            todo.set_done(true);
            Ok(())
        })
        .await
        .expect_err("stale update should conflict");
    let err_text = err.to_string().to_lowercase();
    assert!(
        err_text.contains("conflict(") && err_text.contains("optimistic_lock"),
        "unexpected error text: {err}"
    );

    assert_eq!(
        stale.get(&todo_id).map(|todo| *todo.done()),
        Some(false),
        "failed stale update must not leave partial in-memory mutations"
    );
}

#[tokio::test]
async fn managed_collection_continues_from_legacy_snapshot_without_manual_restore() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_legacy_migration_path");

    let app = PersistApp::open_auto(root.clone())
        .await
        .expect("open legacy app");
    let mut legacy = app
        .open_vec_legacy::<AppTodoVec>("todo_migration")
        .await
        .expect("open legacy vec");
    legacy.add_one(AppTodo::new("Legacy seed".to_string(), false));
    legacy.save_all().await.expect("legacy save");
    assert_eq!(legacy.len(), 1);
    drop(legacy);

    let app_new = PersistApp::open_auto(root.clone())
        .await
        .expect("open managed app");
    let mut managed = app_new
        .open_vec::<AppTodoVec>("todo_migration")
        .await
        .expect("open managed vec");
    assert_eq!(managed.list().len(), 1);
    let todo_id = managed.list()[0].persist_id().to_string();

    let updated = managed
        .apply_command(&todo_id, AppTodoCommand::SetDone(true))
        .await
        .expect("apply command after legacy migration");
    assert!(updated);
    assert_eq!(managed.get(&todo_id).map(|todo| *todo.done()), Some(true));
    drop(managed);

    let app_recovered = PersistApp::open_auto(root)
        .await
        .expect("open recovered app");
    let recovered = app_recovered
        .open_vec::<AppTodoVec>("todo_migration")
        .await
        .expect("open recovered vec");
    assert_eq!(recovered.list().len(), 1);
    assert_eq!(recovered.get(&todo_id).map(|todo| *todo.done()), Some(true));
}
