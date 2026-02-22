use std::sync::Arc;

use habit_hero_classic_api::{
    application::user_service::UserService, build_router,
    infrastructure::in_memory_user_repository::InMemoryUserRepository, state::AppState,
};
use habit_hero_shared_tests::{
    run_commands_contract, run_create_user_contract, run_health_contract, run_read_users_contract,
    run_write_users_contract,
};

#[tokio::test]
async fn user_contract_matches_openapi_expectations() {
    let repository = Arc::new(InMemoryUserRepository::new());
    let service = Arc::new(UserService::new(repository));
    let state = AppState::new(service);

    let app = build_router(state);

    run_health_contract(app.clone()).await;
    run_create_user_contract(app.clone()).await;
    run_read_users_contract(app.clone()).await;
    run_write_users_contract(app.clone()).await;
    run_commands_contract(app).await;
}
