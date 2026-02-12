use rustmemodb::{
    DbError, PersistApp, PersistAppPolicy, PersistEntity, PersistEntityFactory,
    PersistReplicationMode, PersistReplicationPolicy, Value, persist_struct, persist_vec,
};

persist_struct! {
    pub struct AppTodo {
        title: String,
        done: bool,
    }
}

persist_vec!(pub AppTodoVec, AppTodo);

persist_struct! {
    pub struct DynamicAppTodo from_ddl = "CREATE TABLE dynamic_app_todo (title TEXT NOT NULL, done BOOLEAN)"
}

persist_vec!(pub DynamicAppTodoVec, DynamicAppTodo);

#[tokio::test]
async fn persist_app_open_vec_mutate_and_recover_from_snapshot() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_app_primary");

    let policy = PersistAppPolicy {
        snapshot_every_ops: 1,
        replication: PersistReplicationPolicy::default(),
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
