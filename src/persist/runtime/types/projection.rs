// Projection types are split by concern to keep runtime type contracts maintainable.
include!("projection/contracts.rs");
include!("projection/table_and_undo.rs");
include!("projection/mailbox.rs");
