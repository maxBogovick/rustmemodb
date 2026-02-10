use std::sync::Arc;

use crate::repository::TodoRepository;

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<dyn TodoRepository>,
}

impl AppState {
    pub fn new(repo: Arc<dyn TodoRepository>) -> Self {
        Self { repo }
    }
}
