// Runtime storage/projection internals are split to keep responsibilities explicit.

include!("storage_and_projection/disk_and_journal.rs");
include!("storage_and_projection/projections.rs");
include!("storage_and_projection/mailboxes.rs");
