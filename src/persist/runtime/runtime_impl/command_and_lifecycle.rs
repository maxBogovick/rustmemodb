// Runtime command + lifecycle flow is split into focused chunks.

include!("command_and_lifecycle/deterministic_command.rs");
include!("command_and_lifecycle/runtime_closure.rs");
include!("command_and_lifecycle/lifecycle_snapshot.rs");
