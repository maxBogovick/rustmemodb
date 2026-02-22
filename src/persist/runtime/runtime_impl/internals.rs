// Runtime internals are split to keep infra mechanics isolated and navigable.

include!("internals/entity_and_tombstones.rs");
include!("internals/journal_and_snapshot.rs");
include!("internals/replication_and_io.rs");
include!("internals/recovery_and_backpressure.rs");
