use chrono::{Duration, Utc};
use rustmemodb::{
    InMemoryDB, InvokeStatus, PersistCommandModel, PersistEntity, PersistMigrationPlan,
    PersistMigrationStep, PersistModel, PersistSession, PersistValue, RestoreConflictPolicy,
    SnapshotMode, Value, persist_struct, persist_vec,
};

persist_struct! {
    pub struct PersistUser {
        name: String,
        score: i64,
        active: bool,
    }
}

persist_struct! {
    pub struct DdlNote from_ddl = "CREATE TABLE source_note (title TEXT NOT NULL, score INTEGER, active BOOLEAN)"
}

persist_struct! {
    pub struct JsonNote from_json_schema = r#"{
        "type": "object",
        "properties": {
            "title": { "type": "string" },
            "count": { "type": "integer" },
            "flag": { "type": "boolean" }
        },
        "required": ["title"]
    }"#
}

persist_vec!(pub PersistUserVec, PersistUser);
persist_vec!(pub DdlNoteVec, DdlNote);
persist_vec!(hetero pub MixedPersistVec);

#[derive(PersistModel)]
#[persist_model(schema_version = 2)]
struct TaskModel {
    title: String,
    done: bool,
    attempts: i64,
}

persist_struct!(pub struct PersistedTask from_struct = TaskModel);

#[tokio::test]
async fn persist_struct_saves_and_updates_only_changed_fields() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut user = PersistUser::new("Alice".to_string(), 10, true);

    user.save(&session).await.unwrap();

    let table = user.table_name().to_string();
    let persisted_id = user.persist_id().to_string();
    let selected = session
        .query(&format!(
            "SELECT name, score, active FROM {} WHERE __persist_id = '{}'",
            table, persisted_id
        ))
        .await
        .unwrap();

    assert_eq!(selected.row_count(), 1);
    assert_eq!(selected.rows()[0][0], Value::Text("Alice".to_string()));
    assert_eq!(selected.rows()[0][1], Value::Integer(10));
    assert_eq!(selected.rows()[0][2], Value::Boolean(true));

    user.set_score(25);
    user.save(&session).await.unwrap();

    let selected = session
        .query(&format!(
            "SELECT score FROM {} WHERE __persist_id = '{}'",
            user.table_name(),
            user.persist_id()
        ))
        .await
        .unwrap();

    assert_eq!(selected.row_count(), 1);
    assert_eq!(selected.rows()[0][0], Value::Integer(25));
}

#[tokio::test]
async fn persist_struct_auto_persist_works_with_bound_session() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut user = PersistUser::new("Auto".to_string(), 1, true);

    user.bind_session(session.clone());
    user.set_auto_persist(true).unwrap();

    let changed = user.set_score_persisted(2).await.unwrap();
    assert!(changed);
    assert!(user.metadata().persisted);

    let selected = session
        .query(&format!(
            "SELECT score, active FROM {} WHERE __persist_id = '{}'",
            user.table_name(),
            user.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(selected.row_count(), 1);
    assert_eq!(selected.rows()[0][0], Value::Integer(2));

    user.mutate_persisted(|u| u.set_active(false))
        .await
        .unwrap();
    let selected = session
        .query(&format!(
            "SELECT active FROM {} WHERE __persist_id = '{}'",
            user.table_name(),
            user.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(selected.rows()[0][0], Value::Boolean(false));
}

#[tokio::test]
async fn persist_struct_reports_and_executes_custom_functions() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut user = PersistUser::new("Bob".to_string(), 7, true);

    user.register_function("double_score", |obj, _args| {
        let next = obj.score() * 2;
        obj.set_score(next);
        Ok(Value::Integer(next))
    });

    let functions = user.available_functions();
    assert!(functions.iter().any(|f| f.name == "double_score"));

    let result = user.invoke("double_score", vec![], &session).await.unwrap();
    assert_eq!(result, Value::Integer(14));

    user.save(&session).await.unwrap();
}

#[tokio::test]
async fn persist_vec_snapshot_restore_and_prune() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut users = PersistUserVec::new("users");

    let user_a = PersistUser::new("Alice".to_string(), 10, true);
    let user_b = PersistUser::new("Bob".to_string(), 20, true);
    users.add_many(vec![user_a, user_b]);

    users.save_all(&session).await.unwrap();
    assert_eq!(users.len(), 2);

    let snapshot = users.snapshot(SnapshotMode::WithData);

    let restore_session = PersistSession::new(InMemoryDB::new());
    let mut restored = PersistUserVec::new("restored");
    restored.restore(snapshot, &restore_session).await.unwrap();

    assert_eq!(restored.len(), 2);
    let restored_states = restored.states();
    assert_eq!(restored_states.len(), 2);

    let restored_table = restored_states[0].table_name.clone();
    let rows = restore_session
        .query(&format!("SELECT * FROM {}", restored_table))
        .await
        .unwrap();
    assert_eq!(rows.row_count(), 2);

    let mut stale = PersistUser::new("Stale".to_string(), 0, false);
    let meta = stale.metadata_mut();
    meta.created_at = Utc::now() - Duration::hours(48);
    users.add_one(stale);

    let removed = users
        .prune_stale(Duration::hours(1), &session)
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn persist_struct_from_ddl_saves_updates_and_reports_functions() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut note = DdlNote::new().unwrap();

    note.set_field("title", Value::Text("hello".to_string()))
        .unwrap();
    note.set_field("score", Value::Integer(10)).unwrap();
    note.save(&session).await.unwrap();

    let rows = session
        .query(&format!(
            "SELECT title, score FROM {} WHERE __persist_id = '{}'",
            note.table_name(),
            note.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(rows.row_count(), 1);
    assert_eq!(rows.rows()[0][0], Value::Text("hello".to_string()));
    assert_eq!(rows.rows()[0][1], Value::Integer(10));

    note.set_field("score", Value::Integer(20)).unwrap();
    note.save(&session).await.unwrap();
    let rows = session
        .query(&format!(
            "SELECT score FROM {} WHERE __persist_id = '{}'",
            note.table_name(),
            note.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(rows.rows()[0][0], Value::Integer(20));

    let functions = note.available_functions();
    assert!(functions.iter().any(|f| f.name == "state"));
    assert!(functions.iter().any(|f| f.name == "save"));
}

#[tokio::test]
async fn dynamic_struct_auto_persist_works_with_bound_session() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut note = DdlNote::new().unwrap();

    note.bind_session(session.clone());
    note.set_auto_persist(true).unwrap();
    note.set_field_persisted("title", Value::Text("auto-ddl".to_string()))
        .await
        .unwrap();
    note.set_field_persisted("score", Value::Integer(99))
        .await
        .unwrap();

    let rows = session
        .query(&format!(
            "SELECT title, score FROM {} WHERE __persist_id = '{}'",
            note.table_name(),
            note.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(rows.row_count(), 1);
    assert_eq!(rows.rows()[0][0], Value::Text("auto-ddl".to_string()));
    assert_eq!(rows.rows()[0][1], Value::Integer(99));
}

#[tokio::test]
async fn persist_struct_dynamic_command_first_api_works() {
    let session = PersistSession::new(InMemoryDB::new());

    let draft = DdlNoteDraft::new()
        .with_field("title", Value::Text("draft-ddl".to_string()))
        .unwrap()
        .with_field("score", Value::Integer(5))
        .unwrap();
    let mut note = <DdlNote as PersistCommandModel>::try_from_draft(draft).unwrap();

    let changed = note
        .apply(DdlNoteCommand::set("score", Value::Integer(8)))
        .unwrap();
    assert!(changed);

    let changed = note
        .patch(
            DdlNotePatch::new()
                .with_field("active", Value::Boolean(true))
                .unwrap(),
        )
        .unwrap();
    assert!(changed);

    note.save(&session).await.unwrap();

    let rows = session
        .query(&format!(
            "SELECT title, score, active FROM {} WHERE __persist_id = '{}'",
            note.table_name(),
            note.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(rows.rows()[0][0], Value::Text("draft-ddl".to_string()));
    assert_eq!(rows.rows()[0][1], Value::Integer(8));
    assert_eq!(rows.rows()[0][2], Value::Boolean(true));

    let patch_contract = <DdlNote as PersistCommandModel>::patch_contract();
    assert!(patch_contract.iter().any(|field| field.field == "title"));
    let command_contract = <DdlNote as PersistCommandModel>::command_contract();
    assert!(command_contract.iter().any(|cmd| cmd.name == "SetField"));

    let missing_required =
        <DdlNote as PersistCommandModel>::validate_draft_payload(&DdlNoteDraft::new()).unwrap_err();
    assert!(
        missing_required
            .to_string()
            .contains("non-null field 'title'")
    );

    let invalid_type = <DdlNote as PersistCommandModel>::validate_command_payload(
        &DdlNoteCommand::set("score", Value::Text("bad".to_string())),
    )
    .unwrap_err();
    assert!(invalid_type.to_string().contains("expects SQL type"));

    let empty_patch = DdlNotePatch::new().validate().unwrap_err();
    assert!(empty_patch.to_string().contains("Patch payload"));
}

#[tokio::test]
async fn persist_struct_command_first_api_works_for_typed_entities() {
    let session = PersistSession::new(InMemoryDB::new());

    let draft = PersistUserDraft::new("Draft user".to_string(), 1, true);
    let mut user = PersistUser::from_draft(draft);

    let changed = user.apply(PersistUserCommand::SetScore(10)).unwrap();
    assert!(changed);

    let changed = user
        .patch(PersistUserPatch {
            active: Some(false),
            ..Default::default()
        })
        .unwrap();
    assert!(changed);

    let empty_patch_err = user.patch(PersistUserPatch::default()).unwrap_err();
    assert!(empty_patch_err.to_string().contains("Patch payload"));

    user.save(&session).await.unwrap();

    let rows = session
        .query(&format!(
            "SELECT score, active FROM {} WHERE __persist_id = '{}'",
            user.table_name(),
            user.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(rows.rows()[0][0], Value::Integer(10));
    assert_eq!(rows.rows()[0][1], Value::Boolean(false));

    let patch_contract = <PersistUser as PersistCommandModel>::patch_contract();
    assert!(patch_contract.iter().any(|field| field.field == "score"));
    let command_contract = <PersistUser as PersistCommandModel>::command_contract();
    assert!(command_contract.iter().any(|cmd| cmd.name == "SetScore"));
}

#[tokio::test]
async fn persist_model_derive_exposes_command_first_api_through_from_struct_alias() {
    let session = PersistSession::new(InMemoryDB::new());

    let mut task =
        PersistedTask::from_draft(PersistedTaskDraft::new("Write docs".to_string(), false, 0));
    task.apply(PersistedTaskCommand::SetAttempts(2)).unwrap();
    task.patch(PersistedTaskPatch {
        done: Some(true),
        ..Default::default()
    })
    .unwrap();
    task.save(&session).await.unwrap();

    let rows = session
        .query(&format!(
            "SELECT done, attempts FROM {} WHERE __persist_id = '{}'",
            task.table_name(),
            task.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(rows.rows()[0][0], Value::Boolean(true));
    assert_eq!(rows.rows()[0][1], Value::Integer(2));

    let command_contract = <PersistedTask as PersistCommandModel>::command_contract();
    assert!(command_contract.iter().any(|cmd| cmd.name == "SetDone"));
    assert!(command_contract.iter().any(|cmd| cmd.name == "SetAttempts"));
}

#[tokio::test]
async fn persist_struct_from_json_schema_and_vec_restore_work() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut note = JsonNote::new().unwrap();

    note.set_field("title", Value::Text("json".to_string()))
        .unwrap();
    note.set_field("count", Value::Integer(3)).unwrap();
    note.set_field("flag", Value::Boolean(true)).unwrap();
    note.save(&session).await.unwrap();

    let rows = session
        .query(&format!(
            "SELECT title, count, flag FROM {} WHERE __persist_id = '{}'",
            note.table_name(),
            note.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(rows.row_count(), 1);
    assert_eq!(rows.rows()[0][0], Value::Text("json".to_string()));
    assert_eq!(rows.rows()[0][1], Value::Integer(3));
    assert_eq!(rows.rows()[0][2], Value::Boolean(true));

    let mut notes = DdlNoteVec::new("ddl_notes");
    let mut a = DdlNote::new().unwrap();
    a.set_field("title", Value::Text("a".to_string())).unwrap();
    a.set_field("score", Value::Integer(1)).unwrap();
    let mut b = DdlNote::new().unwrap();
    b.set_field("title", Value::Text("b".to_string())).unwrap();
    b.set_field("score", Value::Integer(2)).unwrap();
    notes.add_many(vec![a, b]);
    notes.save_all(&session).await.unwrap();

    let snapshot = notes.snapshot(SnapshotMode::WithData);
    let restore_session = PersistSession::new(InMemoryDB::new());
    let mut restored = DdlNoteVec::new("restore");
    restored.restore(snapshot, &restore_session).await.unwrap();

    assert_eq!(restored.len(), 2);
    let state_count = restored.states().len();
    assert_eq!(state_count, 2);
}

#[test]
fn persist_value_sql_literals_are_available_for_options() {
    let value: Option<i64> = Some(42);
    assert_eq!(value.to_sql_literal(), "42");

    let none_value: Option<i64> = None;
    assert_eq!(none_value.to_sql_literal(), "NULL");
}

#[tokio::test]
async fn persist_vec_restore_conflict_policies_work() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut notes = DdlNoteVec::new("notes");

    let mut item = DdlNote::new().unwrap();
    item.set_field("title", Value::Text("base".to_string()))
        .unwrap();
    item.save(&session).await.unwrap();
    notes.add_one(item);

    let snapshot = notes.snapshot(SnapshotMode::WithData);

    let mut fail_fast_restore = DdlNoteVec::new("ff");
    let err = fail_fast_restore.restore(snapshot.clone(), &session).await;
    assert!(err.is_err());

    let mut skip_restore = DdlNoteVec::new("skip");
    skip_restore
        .restore_with_policy(
            snapshot.clone(),
            &session,
            RestoreConflictPolicy::SkipExisting,
        )
        .await
        .unwrap();
    assert_eq!(skip_restore.len(), 0);

    let mut overwrite_restore = DdlNoteVec::new("ow");
    overwrite_restore
        .restore_with_policy(snapshot, &session, RestoreConflictPolicy::OverwriteExisting)
        .await
        .unwrap();
    assert_eq!(overwrite_restore.len(), 1);
}

#[tokio::test]
async fn heterogeneous_persist_vec_supports_mixed_types_and_selective_invoke() {
    let session = PersistSession::new(InMemoryDB::new());

    let mut mixed = MixedPersistVec::new("mixed");
    mixed.register_type::<PersistUser>();
    mixed.register_type::<DdlNote>();

    let mut user = PersistUser::new("Mia".to_string(), 5, true);
    user.register_function("double_score", |obj, _args| {
        let next = *obj.score() * 2;
        obj.set_score(next);
        Ok(Value::Integer(next))
    });

    let mut note = DdlNote::new().unwrap();
    note.set_field("title", Value::Text("note".to_string()))
        .unwrap();
    note.set_field("score", Value::Integer(9)).unwrap();

    mixed.add_many(vec![user]).unwrap();
    mixed.add_one(note).unwrap();

    mixed.save_all(&session).await.unwrap();
    assert_eq!(mixed.len(), 2);

    let outcomes = mixed
        .invoke_supported("double_score", vec![], &session)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);

    let invoked = outcomes
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::Invoked))
        .count();
    let skipped = outcomes
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::SkippedUnsupported))
        .count();

    assert_eq!(invoked, 1);
    assert_eq!(skipped, 1);

    let snapshot = mixed.snapshot(SnapshotMode::WithData);

    let restore_session = PersistSession::new(InMemoryDB::new());
    let mut restored = MixedPersistVec::new("mixed-restored");
    restored.register_type::<PersistUser>();
    restored.register_type::<DdlNote>();
    restored.restore(snapshot, &restore_session).await.unwrap();

    assert_eq!(restored.len(), 2);
    assert_eq!(restored.states().len(), 2);
}

#[tokio::test]
async fn derive_persist_model_and_from_struct_alias_work() {
    let session = PersistSession::new(InMemoryDB::new());

    let model = TaskModel {
        title: "task-a".to_string(),
        done: false,
        attempts: 0,
    };
    let mut task = model.into_persisted();
    task.bind_session(session.clone());
    task.set_auto_persist(true).unwrap();

    task.set_attempts_persisted(1).await.unwrap();
    task.set_done_persisted(true).await.unwrap();
    task.mutate_persisted(|t| t.set_title("task-a-updated".to_string()))
        .await
        .unwrap();
    assert_eq!(task.metadata().schema_version, 2);

    let selected = session
        .query(&format!(
            "SELECT title, done, attempts FROM {} WHERE __persist_id = '{}'",
            task.table_name(),
            task.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(selected.row_count(), 1);
    assert_eq!(
        selected.rows()[0][0],
        Value::Text("task-a-updated".to_string())
    );
    assert_eq!(selected.rows()[0][1], Value::Boolean(true));
    assert_eq!(selected.rows()[0][2], Value::Integer(1));

    let mut from_alias = PersistedTask::from_parts("task-b".to_string(), false, 3);
    from_alias.save(&session).await.unwrap();

    let selected_alias = session
        .query(&format!(
            "SELECT title, done, attempts FROM {} WHERE __persist_id = '{}'",
            from_alias.table_name(),
            from_alias.persist_id()
        ))
        .await
        .unwrap();
    assert_eq!(selected_alias.row_count(), 1);
}

#[tokio::test]
async fn persist_vec_custom_migration_plan_migrates_state_and_schema_version() {
    let source_session = PersistSession::new(InMemoryDB::new());
    let mut source = PersistUserVec::new("users-migration-source");
    source.add_one(PersistUser::new("Legacy".to_string(), 7, true));
    source.save_all(&source_session).await.unwrap();

    let mut snapshot = source.snapshot(SnapshotMode::WithData);
    snapshot.schema_version = 1;
    for state in &mut snapshot.states {
        state.metadata.schema_version = 1;
    }

    let mut migration_plan = PersistMigrationPlan::new(2);
    migration_plan
        .add_step(
            PersistMigrationStep::new(1, 2).with_state_migrator(|state| {
                let fields = state.fields_object_mut()?;
                let score = fields.get("score").and_then(|v| v.as_i64()).unwrap_or(0);
                fields.insert("score".to_string(), serde_json::Value::from(score * 10));
                Ok(())
            }),
        )
        .unwrap();

    let restore_session = PersistSession::new(InMemoryDB::new());
    let mut restored = PersistUserVec::new("users-migration-restored");
    restored
        .restore_with_custom_migration_plan(
            snapshot,
            &restore_session,
            RestoreConflictPolicy::FailFast,
            migration_plan,
        )
        .await
        .unwrap();

    assert_eq!(restored.len(), 1);
    assert_eq!(*restored.items()[0].score(), 70);
    assert_eq!(restored.items()[0].metadata().schema_version, 2);

    let table_version = restore_session
        .get_table_schema_version(restored.items()[0].table_name())
        .await
        .unwrap();
    assert_eq!(table_version, Some(2));
}

#[tokio::test]
async fn heterogeneous_vec_respects_per_type_migration_plan() {
    let source_session = PersistSession::new(InMemoryDB::new());
    let mut mixed = MixedPersistVec::new("mixed-source");
    mixed.register_type::<PersistUser>();
    mixed.register_type::<DdlNote>();

    let user = PersistUser::new("TypeMigrate".to_string(), 5, true);
    let mut note = DdlNote::new().unwrap();
    note.set_field("title", Value::Text("note".to_string()))
        .unwrap();
    note.set_field("score", Value::Integer(1)).unwrap();

    mixed.add_one(user).unwrap();
    mixed.add_one(note).unwrap();
    mixed.save_all(&source_session).await.unwrap();

    let mut snapshot = mixed.snapshot(SnapshotMode::WithData);
    for state in &mut snapshot.states {
        if state.type_name == "PersistUser" {
            state.metadata.schema_version = 1;
        }
    }

    let mut user_plan = PersistMigrationPlan::new(2);
    user_plan
        .add_state_step(1, 2, |state| {
            let fields = state.fields_object_mut()?;
            let score = fields
                .get("score")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("score".to_string(), serde_json::Value::from(score + 100));
            Ok(())
        })
        .unwrap();

    let restore_session = PersistSession::new(InMemoryDB::new());
    let mut restored = MixedPersistVec::new("mixed-restored");
    restored.register_type_with_migration_plan::<PersistUser>(user_plan);
    restored.register_type::<DdlNote>();
    restored.restore(snapshot, &restore_session).await.unwrap();

    let states = restored.states();
    let user_state = states
        .iter()
        .find(|state| state.type_name == "PersistUser")
        .unwrap();
    let user_score = user_state
        .fields
        .as_object()
        .and_then(|obj| obj.get("score"))
        .and_then(|v| v.as_i64())
        .unwrap();
    assert_eq!(user_score, 105);
    assert_eq!(user_state.metadata.schema_version, 2);
}

#[test]
fn heterogeneous_persist_vec_rejects_unregistered_types() {
    let mut mixed = MixedPersistVec::new("mixed");
    let user = PersistUser::new("NoReg".to_string(), 1, true);
    let err = mixed.add_one(user).unwrap_err();
    assert!(err.to_string().contains("not registered"));
}
