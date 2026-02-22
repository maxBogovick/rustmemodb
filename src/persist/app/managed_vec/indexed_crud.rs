// Indexed CRUD is split by operation to keep persistence workflows easy to navigate.
include!("indexed_crud/validation_and_reads.rs");
include!("indexed_crud/create_paths.rs");
include!("indexed_crud/update_paths.rs");
include!("indexed_crud/delete_paths.rs");
