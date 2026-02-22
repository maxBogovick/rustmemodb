/// Type alias for a simple deterministic command handler.
pub type DeterministicCommandHandler =
    Arc<dyn Fn(&mut PersistState, &serde_json::Value) -> Result<()> + Send + Sync>;

/// Type alias for a deterministic command handler that consumes an envelope and produces side effects.
pub type DeterministicEnvelopeCommandHandler = Arc<
    dyn Fn(&mut PersistState, &RuntimeCommandEnvelope) -> Result<Vec<RuntimeSideEffectSpec>>
        + Send
        + Sync,
>;

/// Type alias for a deterministic command handler that uses a context and produces side effects.
pub type DeterministicContextCommandHandler = Arc<
    dyn Fn(
            &mut PersistState,
            &serde_json::Value,
            &RuntimeDeterministicContext,
        ) -> Result<Vec<RuntimeSideEffectSpec>>
        + Send
        + Sync,
>;

/// Type alias for generic runtime closures (dynamic logic).
pub type RuntimeClosureHandler =
    Arc<dyn Fn(&mut PersistState, Vec<Value>) -> Result<Value> + Send + Sync>;

/// Type alias for command payload migration functions.
pub type RuntimeCommandPayloadMigration =
    Arc<dyn Fn(&serde_json::Value) -> Result<serde_json::Value> + Send + Sync>;
