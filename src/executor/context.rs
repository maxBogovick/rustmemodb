use crate::storage::InMemoryStorage;

pub struct ExecutionContext<'a> {
    pub storage: &'a InMemoryStorage,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(storage: &'a InMemoryStorage) -> Self {
        Self { storage }
    }
}
