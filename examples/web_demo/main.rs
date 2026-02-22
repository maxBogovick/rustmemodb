use rustmemodb::web::WebError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateUser {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub name: String,
}

#[rustmemodb::web::api_service]
pub trait UserApi {
    async fn create_user(&self, cmd: CreateUser) -> Result<User, WebError>;
    async fn get_user(&self, id: String) -> Result<User, WebError>;
}

#[derive(Clone)]
struct UserStore;

impl UserApi for UserStore {
    async fn create_user(&self, cmd: CreateUser) -> Result<User, WebError> {
        Ok(User {
            id: "1".to_string(),
            name: cmd.name,
        })
    }

    async fn get_user(&self, id: String) -> Result<User, WebError> {
        Ok(User {
            id,
            name: "Test".to_string(),
        })
    }
}

#[tokio::main]
async fn main() {
    let store = UserStore;
    let app = UserApiServer::new(store).router();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
