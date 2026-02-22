use std::sync::Arc;

use habit_hero_product_api::{
    application::{dto::CreateUserRequest, user_service::UserService},
    domain::errors::DomainError,
    infrastructure::persist_user_store::PersistUserStore,
};
use tempfile::tempdir;

#[tokio::test]
async fn email_uniqueness_is_enforced_across_store_reopen() {
    let temp = tempdir().expect("temp dir should be created");
    let data_dir = temp.path().join("persist");

    let service_a = UserService::new(Arc::new(
        PersistUserStore::open(data_dir.clone())
            .await
            .expect("first store should initialize"),
    ));

    service_a
        .create_user(CreateUserRequest {
            email: "alice@example.com".to_string(),
            display_name: "Alice".to_string(),
        })
        .await
        .expect("first create should succeed");

    drop(service_a);

    let service_b = UserService::new(Arc::new(
        PersistUserStore::open(data_dir)
            .await
            .expect("second store should initialize"),
    ));

    let duplicate_error = service_b
        .create_user(CreateUserRequest {
            email: "alice@example.com".to_string(),
            display_name: "Alice Second".to_string(),
        })
        .await
        .expect_err("duplicate email must fail on reopened store");

    assert!(matches!(duplicate_error, DomainError::Conflict(_)));
}
