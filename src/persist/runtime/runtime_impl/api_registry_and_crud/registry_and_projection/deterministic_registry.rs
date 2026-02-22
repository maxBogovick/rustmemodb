impl PersistEntityRuntime {
    /// Registers a legacy deterministic command handler (no envelope).
    pub fn register_deterministic_command(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        handler: DeterministicCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Legacy(handler),
                payload_schema: None,
            },
        );
    }

    /// Registers a legacy deterministic command handler with a payload schema.
    pub fn register_deterministic_command_with_schema(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Legacy(handler),
                payload_schema: Some(payload_schema),
            },
        );
    }

    /// Registers a deterministic command handler that receives the full command envelope.
    pub fn register_deterministic_envelope_command(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        handler: DeterministicEnvelopeCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Envelope(handler),
                payload_schema: None,
            },
        );
    }

    /// Registers an envelope-based command handler with a payload schema.
    pub fn register_deterministic_envelope_command_with_schema(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicEnvelopeCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Envelope(handler),
                payload_schema: Some(payload_schema),
            },
        );
    }

    /// Registers a deterministic command handler that receives an execution context.
    pub fn register_deterministic_context_command(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        handler: DeterministicContextCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Context(handler),
                payload_schema: None,
            },
        );
    }

    /// Registers a context-based command handler with a payload schema.
    pub fn register_deterministic_context_command_with_schema(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicContextCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Context(handler),
                payload_schema: Some(payload_schema),
            },
        );
    }
}
