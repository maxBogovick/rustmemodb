use rustmemodb::{
    Autonomous, PersistApp, PersistDomainError, PersistDomainMutationError, autonomous_impl,
};

#[derive(Debug, Clone, PartialEq, Eq, Autonomous)]
#[persist_model(table = "autonomous_board_model", schema_version = 1)]
struct AutonomousBoard {
    name: String,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoardMutatorError {
    EmptyName,
}

impl std::fmt::Display for BoardMutatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName => write!(f, "board name must not be empty"),
        }
    }
}

impl std::error::Error for BoardMutatorError {}

#[autonomous_impl]
impl AutonomousBoard {
    #[rustmemodb::command]
    fn rename_checked(&mut self, name: String) -> Result<String, BoardMutatorError> {
        let normalized = name.trim().to_string();
        if normalized.is_empty() {
            return Err(BoardMutatorError::EmptyName);
        }
        self.name = normalized.clone();
        Ok(normalized)
    }

    #[rustmemodb::command]
    fn deactivate_flag(&mut self) -> bool {
        self.active = false;
        self.active
    }
}

#[tokio::test]
async fn autonomous_derive_opens_domain_by_source_model_type() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_autonomous_derive_domain");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let boards = app
        .open_autonomous_model::<AutonomousBoard>("boards_autonomous")
        .await
        .expect("open autonomous model handle");

    let created = boards
        .create_one(AutonomousBoard {
            name: "Platform Team".to_string(),
            active: true,
        })
        .await
        .expect("create one");
    assert_eq!(created.model.name, "Platform Team");
    assert!(created.model.active);
    assert!(!created.persist_id.is_empty());
    assert_eq!(created.version, 1);

    let fetched = boards
        .get_one(&created.persist_id)
        .await
        .expect("get one should return created entity");
    assert_eq!(fetched.model, created.model);
    assert_eq!(fetched.persist_id, created.persist_id);

    let listed = boards.list().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].persist_id, created.persist_id);
}

#[tokio::test]
async fn autonomous_derive_mutate_preserves_user_errors_and_rolls_back() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_autonomous_derive_mutate");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let boards = app
        .open_autonomous_model::<AutonomousBoard>("boards_autonomous_mutate")
        .await
        .expect("open autonomous model handle");

    let created = boards
        .create_one(AutonomousBoard {
            name: "Initial".to_string(),
            active: true,
        })
        .await
        .expect("create one");

    let (updated, transition) = boards
        .mutate_one_with_result(&created.persist_id, |board| {
            board.name = "Updated".to_string();
            board.active = false;
            Ok::<&'static str, BoardMutatorError>("applied")
        })
        .await
        .expect("mutate one should succeed");
    assert_eq!(transition, "applied");
    assert_eq!(updated.model.name, "Updated");
    assert!(!updated.model.active);
    assert!(updated.version >= created.version);

    let user_error = boards
        .mutate_one_with(&created.persist_id, |_board| {
            Err(BoardMutatorError::EmptyName)
        })
        .await
        .expect_err("mutate one should return user error");
    assert_eq!(
        user_error,
        PersistDomainMutationError::User(BoardMutatorError::EmptyName)
    );

    let unchanged = boards
        .get_one(&created.persist_id)
        .await
        .expect("record should remain after rollback");
    assert_eq!(unchanged.model.name, "Updated");
    assert!(!unchanged.model.active);
    assert_eq!(unchanged.version, updated.version);

    boards
        .remove_one(&created.persist_id)
        .await
        .expect("remove one should succeed");
    let missing_remove = boards
        .remove_one(&created.persist_id)
        .await
        .expect_err("second remove should be not found");
    assert_eq!(missing_remove, PersistDomainError::NotFound);
}

#[tokio::test]
async fn autonomous_impl_generates_domain_handle_methods() {
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().join("persist_autonomous_impl_derive");

    let app = PersistApp::open_auto(root).await.expect("open app");
    let boards = app
        .open_autonomous_model::<AutonomousBoard>("boards_autonomous_impl")
        .await
        .expect("open autonomous model handle");

    let created = boards
        .create_one(AutonomousBoard {
            name: "Initial".to_string(),
            active: true,
        })
        .await
        .expect("create one");

    let renamed = boards
        .rename_checked(&created.persist_id, "Updated".to_string())
        .await
        .expect("rename through generated method");
    assert_eq!(renamed, "Updated");

    let rename_error = boards
        .rename_checked(&created.persist_id, "   ".to_string())
        .await
        .expect_err("empty rename should return business error");
    assert_eq!(
        rename_error,
        PersistDomainMutationError::User(BoardMutatorError::EmptyName)
    );

    let active = boards
        .deactivate_flag(&created.persist_id)
        .await
        .expect("deactivate through generated method");
    assert!(!active);

    let audits = boards
        .domain_handle()
        .list_audits_for(&created.persist_id)
        .await;
    assert!(
        audits
            .iter()
            .any(|event| event.event_type() == "rename_checked")
    );
    assert!(
        audits
            .iter()
            .any(|event| event.event_type() == "deactivate_flag")
    );
}

#[test]
fn autonomous_derive_generates_collection_type() {
    let mut vec = AutonomousBoardAutonomousVec::new("autonomous_vec_smoke");
    let item = AutonomousBoardPersisted::new(AutonomousBoard {
        name: "Generated".to_string(),
        active: true,
    });
    let id = item.persist_id().to_string();
    vec.add_one(item);
    assert_eq!(vec.len(), 1);
    assert_eq!(vec.items().len(), 1);
    assert_eq!(vec.items()[0].persist_id(), id);
    assert_eq!(vec.items()[0].name(), "Generated");
}
