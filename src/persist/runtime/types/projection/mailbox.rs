/// Tracks the state of an entity's command processing mailbox.
///
/// Used for per-entity concurrency control (serializing commands).
#[derive(Debug, Clone)]
struct RuntimeEntityMailbox {
    /// Number of commands waiting in the queue (if we had a queue, but here just tracking load).
    pending_commands: u64,
    /// Whether a command is currently executing.
    inflight: bool,
    /// Timestamp of the last processed command.
    last_command_at: DateTime<Utc>,
}

impl RuntimeEntityMailbox {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            pending_commands: 0,
            inflight: false,
            last_command_at: now,
        }
    }
}
