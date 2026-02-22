// Shard routing is split by concern to keep cluster internals maintainable.
include!("routing/types.rs");
include!("routing/membership.rs");
include!("routing/routing_table.rs");
include!("routing/shard_hash.rs");
