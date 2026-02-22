use std::sync::Arc;

use habit_hero_product_api::{
    application::user_service::UserService, build_router, state::AppState,
};
use habit_hero_shared_tests::{
    run_commands_contract_dx, run_create_user_contract, run_health_contract,
    run_read_users_contract, run_write_users_contract_dx,
};
use tempfile::tempdir;

#[tokio::test]
async fn user_contract_matches_openapi_expectations() {
    let temp = tempdir().expect("temp dir should be created");

    let service = Arc::new(
        UserService::open(temp.path().join("persist"))
            .await
            .expect("user service should initialize"),
    );
    let state = AppState::new(service);

    let app = build_router(state);

    run_health_contract(app.clone()).await;
    run_create_user_contract(app.clone()).await;
    run_read_users_contract(app.clone()).await;
    run_write_users_contract_dx(app.clone()).await;
    run_commands_contract_dx(app).await;
}
