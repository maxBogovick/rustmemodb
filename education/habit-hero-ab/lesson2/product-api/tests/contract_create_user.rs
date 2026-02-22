use std::sync::Arc;

use habit_hero_product_api::{
    application::user_service::UserService, build_router,
    infrastructure::persist_user_store::PersistUserStore, state::AppState,
};
use habit_hero_shared_tests::{
    run_create_user_contract, run_health_contract, run_read_users_contract,
};
use tempfile::tempdir;

#[tokio::test]
async fn user_contract_matches_openapi_expectations() {
    let temp = tempdir().expect("temp dir should be created");

    let repository = Arc::new(
        PersistUserStore::open(temp.path().join("persist"))
            .await
            .expect("persist store should initialize"),
    );
    let service = Arc::new(UserService::new(repository));
    let state = AppState::new(service);

    let app = build_router(state);

    run_health_contract(app.clone()).await;
    run_create_user_contract(app.clone()).await;
    run_read_users_contract(app).await;
}
