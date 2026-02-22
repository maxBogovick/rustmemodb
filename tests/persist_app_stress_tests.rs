use rustmemodb::{
    DbError, ManagedConflictKind, PersistApp, PersistAppPolicy, PersistAutonomousIntent,
    PersistConflictRetryPolicy, Value, classify_managed_conflict, persist_struct, persist_vec,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Mutex};
use tokio::time::{Duration, timeout};

persist_struct! {
    pub struct StressUser {
        name: String,
        active: bool,
    }
}
persist_vec!(pub StressUserVec, StressUser);

persist_struct! {
    pub struct StressLedgerEntry {
        label: String,
        amount: i64,
    }
}
persist_vec!(pub StressLedgerEntryVec, StressLedgerEntry);

persist_struct! {
    pub struct StressLedgerAudit {
        label: String,
        amount: i64,
    }
}
persist_vec!(pub StressLedgerAuditVec, StressLedgerAudit);

persist_struct! {
    pub struct StressLifecycleUser {
        email: String,
        active: bool,
    }
}
persist_vec!(pub StressLifecycleUserVec, StressLifecycleUser);

#[derive(Clone, Copy, PersistAutonomousIntent)]
#[persist_intent(model = StressLifecycleUser)]
enum StressLifecycleCommand {
    #[persist_case(command = StressLifecycleUserCommand::SetActive(true))]
    Activate,
    #[persist_case(command = StressLifecycleUserCommand::SetActive(false))]
    Deactivate,
}

#[derive(Debug)]
struct WriteWriteRaceOutcome {
    workers: usize,
    attempts: usize,
    successes: usize,
    conflicts: usize,
    other_errors: Vec<String>,
    final_value: i64,
}

async fn run_write_write_race(
    retry_policy: PersistConflictRetryPolicy,
    workers: usize,
) -> WriteWriteRaceOutcome {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_write_write_race");
    let app = PersistApp::open(
        root,
        PersistAppPolicy {
            conflict_retry: retry_policy,
            ..PersistAppPolicy::default()
        },
    )
    .await
    .expect("open app");

    app.transaction(|tx| async move {
        tx.execute("CREATE TABLE stress_counter (id INTEGER PRIMARY KEY, v INTEGER)")
            .await?;
        tx.execute("INSERT INTO stress_counter VALUES (1, 0)")
            .await?;
        Ok::<(), DbError>(())
    })
    .await
    .expect("init stress_counter");

    let barrier = Arc::new(Barrier::new(workers));
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let app = app.clone();
        let barrier = barrier.clone();
        let attempts = attempts.clone();
        let first_attempt = Arc::new(AtomicBool::new(true));

        handles.push(tokio::spawn(async move {
            app.transaction(move |tx| {
                let barrier = barrier.clone();
                let attempts = attempts.clone();
                let first_attempt = first_attempt.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    tx.execute("UPDATE stress_counter SET v = v + 1 WHERE id = 1")
                        .await?;
                    if first_attempt.swap(false, Ordering::SeqCst) {
                        // Synchronize first attempt to maximize overlapping commits.
                        barrier.wait().await;
                    }
                    Ok::<(), DbError>(())
                }
            })
            .await
        }));
    }

    let mut successes = 0usize;
    let mut conflicts = 0usize;
    let mut other_errors = Vec::new();

    for handle in handles {
        match handle.await.expect("task must join") {
            Ok(()) => successes += 1,
            Err(err) => match classify_managed_conflict(&err) {
                Some(ManagedConflictKind::WriteWrite) => conflicts += 1,
                _ => other_errors.push(err.to_string()),
            },
        }
    }

    let final_value = app
        .transaction(|tx| async move {
            let result = tx
                .query("SELECT v FROM stress_counter WHERE id = 1")
                .await?;
            match result.rows().first().and_then(|row| row.first()) {
                Some(Value::Integer(value)) => Ok(*value),
                other => Err(DbError::ExecutionError(format!(
                    "unexpected stress_counter payload: {other:?}"
                ))),
            }
        })
        .await
        .expect("read final counter");

    WriteWriteRaceOutcome {
        workers,
        attempts: attempts.load(Ordering::SeqCst),
        successes,
        conflicts,
        other_errors,
        final_value,
    }
}

#[tokio::test]
async fn write_write_retry_disabled_conflicts_surface() {
    timeout(Duration::from_secs(30), async {
        let workers = 16usize;
        let outcome = run_write_write_race(
            PersistConflictRetryPolicy {
                max_attempts: 1,
                base_backoff_ms: 1,
                max_backoff_ms: 2,
                retry_write_write: false,
            },
            workers,
        )
        .await;

        assert_eq!(outcome.workers, workers);
        assert!(
            outcome.conflicts > 0,
            "synchronized first-attempt race should surface write-write conflicts without retry"
        );
        assert!(
            outcome.other_errors.is_empty(),
            "unexpected non-conflict errors: {:?}",
            outcome.other_errors
        );
        assert_eq!(outcome.successes + outcome.conflicts, workers);
        assert_eq!(
            outcome.final_value,
            i64::try_from(outcome.successes).unwrap()
        );
    })
    .await
    .expect("stress test timeout");
}

#[tokio::test]
async fn write_write_retry_enabled_eventual_success() {
    timeout(Duration::from_secs(30), async {
        let workers = 16usize;
        let outcome = run_write_write_race(
            PersistConflictRetryPolicy {
                max_attempts: 32,
                base_backoff_ms: 1,
                max_backoff_ms: 8,
                retry_write_write: true,
            },
            workers,
        )
        .await;

        assert_eq!(outcome.workers, workers);
        assert!(
            outcome.other_errors.is_empty(),
            "unexpected non-conflict errors: {:?}",
            outcome.other_errors
        );
        assert_eq!(
            outcome.successes, workers,
            "all workers should eventually commit under write-write retry policy"
        );
        assert_eq!(outcome.conflicts, 0);
        assert_eq!(outcome.final_value, i64::try_from(workers).unwrap());
        assert!(
            outcome.attempts > workers,
            "expected retry attempts under synchronized conflict race"
        );
    })
    .await
    .expect("stress test timeout");
}

#[tokio::test]
async fn optimistic_lock_is_not_retried_under_load() {
    timeout(Duration::from_secs(30), async {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("persist_app_optimistic_load");
        let app = PersistApp::open(
            root,
            PersistAppPolicy {
                conflict_retry: PersistConflictRetryPolicy {
                    max_attempts: 64,
                    base_backoff_ms: 1,
                    max_backoff_ms: 8,
                    retry_write_write: true,
                },
                ..PersistAppPolicy::default()
            },
        )
        .await
        .expect("open app");

        let mut users = app
            .open_vec::<StressUserVec>("stress_users")
            .await
            .expect("open vec");
        let user = StressUser::new("Initial".to_string(), true);
        let user_id = user.persist_id().to_string();
        users.create(user).await.expect("seed user");

        let users = Arc::new(Mutex::new(users));
        let workers = 12usize;
        let barrier = Arc::new(Barrier::new(workers));
        let mut handles = Vec::with_capacity(workers);

        for idx in 0..workers {
            let users = users.clone();
            let barrier = barrier.clone();
            let user_id = user_id.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let mut users = users.lock().await;
                users
                    .execute_patch_if_match(
                        &user_id,
                        1,
                        StressUserPatch {
                            name: Some(format!("User-{idx}")),
                            ..Default::default()
                        },
                    )
                    .await
            }));
        }

        let mut successes = 0usize;
        let mut optimistic_conflicts = 0usize;
        let mut other_errors = Vec::new();

        for handle in handles {
            match handle.await.expect("task must join") {
                Ok(Some(_updated)) => successes += 1,
                Ok(None) => other_errors.push("unexpected missing entity".to_string()),
                Err(err) => match classify_managed_conflict(&err) {
                    Some(ManagedConflictKind::OptimisticLock) => optimistic_conflicts += 1,
                    _ => other_errors.push(err.to_string()),
                },
            }
        }

        assert!(
            other_errors.is_empty(),
            "unexpected errors: {:?}",
            other_errors
        );
        assert_eq!(
            successes, 1,
            "exactly one stale-if-match competitor must win"
        );
        assert_eq!(optimistic_conflicts, workers - 1);

        let users = users.lock().await;
        let final_user = users
            .get(&user_id)
            .cloned()
            .expect("user must stay present");
        assert_eq!(final_user.metadata().version, 2);
    })
    .await
    .expect("stress test timeout");
}

#[tokio::test]
async fn atomic_with_repeated_failures_has_no_partial_writes() {
    timeout(Duration::from_secs(30), async {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("persist_app_atomic_repeated_failures");
        let app = PersistApp::open_auto(root).await.expect("open app");

        let mut ledger = app
            .open_vec::<StressLedgerEntryVec>("stress_ledger")
            .await
            .expect("open ledger");
        let mut audits = app
            .open_vec::<StressLedgerAuditVec>("stress_ledger_audits")
            .await
            .expect("open audits");

        let mut committed = 0usize;
        for round in 0..25usize {
            let fail_label = format!("fail-{round}");
            let err = ledger
                .atomic_with(&mut audits, move |tx, ledger, audits| {
                    Box::pin(async move {
                        ledger
                            .create_with_tx(&tx, StressLedgerEntry::new(fail_label.clone(), -1))
                            .await?;
                        audits
                            .create_with_tx(&tx, StressLedgerAudit::new(fail_label, -1))
                            .await?;
                        Err::<(), DbError>(DbError::ExecutionError(
                            "stress: injected atomic failure".to_string(),
                        ))
                    })
                })
                .await
                .expect_err("forced failure must rollback both collections");
            assert!(
                err.to_string().contains("stress: injected atomic failure"),
                "unexpected injected failure error: {err}"
            );
            assert_eq!(ledger.list().len(), committed);
            assert_eq!(audits.list().len(), committed);

            let ok_label = format!("ok-{round}");
            ledger
                .atomic_with(&mut audits, move |tx, ledger, audits| {
                    Box::pin(async move {
                        ledger
                            .create_with_tx(&tx, StressLedgerEntry::new(ok_label.clone(), 1))
                            .await?;
                        audits
                            .create_with_tx(&tx, StressLedgerAudit::new(ok_label, 1))
                            .await?;
                        Ok::<(), DbError>(())
                    })
                })
                .await
                .expect("successful atomic write");

            committed += 1;
            assert_eq!(ledger.list().len(), committed);
            assert_eq!(audits.list().len(), committed);
        }
    })
    .await
    .expect("stress test timeout");
}

#[tokio::test]
async fn rollback_then_replay_keeps_consistent_versions() {
    timeout(Duration::from_secs(30), async {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("persist_app_rollback_then_replay");
        let app = PersistApp::open_auto(root).await.expect("open app");

        let mut users = app
            .open_autonomous::<StressLifecycleUserVec>("stress_lifecycle_users")
            .await
            .expect("open autonomous aggregate");
        let user = StressLifecycleUser::new("stress@example.com".to_string(), true);
        let user_id = user.persist_id().to_string();
        users.create(user).await.expect("seed lifecycle user");

        let mut expected_version = 1i64;
        let mut expected_active = true;
        let mut expected_audit_count = 0usize;

        for round in 0..20usize {
            let command = if expected_active {
                StressLifecycleCommand::Deactivate
            } else {
                StressLifecycleCommand::Activate
            };

            let err = match users
                .apply_injected_failure(
                    &user_id,
                    expected_version,
                    command,
                    format!("stress: rollback before audit commit (round {round})"),
                )
                .await
            {
                Ok(_) => panic!("injected failure must rollback"),
                Err(err) => err,
            };
            assert!(
                err.to_string()
                    .contains("stress: rollback before audit commit"),
                "unexpected injected rollback error: {err}"
            );

            let after_failure = users
                .get(&user_id)
                .cloned()
                .expect("user must remain after rollback");
            assert_eq!(after_failure.metadata().version, expected_version);
            assert_eq!(*after_failure.active(), expected_active);
            assert_eq!(users.list_audits_for(&user_id).len(), expected_audit_count);

            let updated = users
                .apply(&user_id, expected_version, command)
                .await
                .expect("replay apply")
                .expect("user must exist");
            expected_version += 1;
            expected_active = !expected_active;
            expected_audit_count += 1;

            assert_eq!(updated.metadata().version, expected_version);
            assert_eq!(*updated.active(), expected_active);
            assert_eq!(users.list_audits_for(&user_id).len(), expected_audit_count);
        }
    })
    .await
    .expect("stress test timeout");
}
