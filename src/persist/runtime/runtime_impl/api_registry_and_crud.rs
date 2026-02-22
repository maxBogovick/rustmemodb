// Runtime API/registry + CRUD is split into focused chunks to keep navigation cheap.

include!("api_registry_and_crud/open_and_stats.rs");
include!("api_registry_and_crud/registry_and_projection.rs");
include!("api_registry_and_crud/entity_crud_and_outbox.rs");
