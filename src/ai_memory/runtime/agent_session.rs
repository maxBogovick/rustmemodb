use crate::ai_memory::banks::episodic::{
    AgentCommandOptions, AgentIncidentForensicsReport, AgentReplayRunOptions, AgentReplayRunReport,
    AgentSessionMemory, AgentSessionRuntimeConfig, AgentSessionTimelineRecord, AgentTimelineQuery,
};
use crate::core::Result;
use crate::{
    DeterministicContextCommandHandler, PersistState, RuntimeCommandEnvelope,
    RuntimeCommandPayloadSchema, RuntimeEnvelopeApplyResult, RuntimeOutboxRecord,
    RuntimeSloMetrics, RuntimeStats,
};

/// Runtime-oriented wrapper around episodic session memory.
///
/// Keeps app-facing usage concise while preserving direct access to the underlying bank.
pub struct AgentSessionRuntime {
    memory: AgentSessionMemory,
}

impl AgentSessionRuntime {
    pub async fn open(config: AgentSessionRuntimeConfig) -> Result<Self> {
        let memory = AgentSessionMemory::open(config).await?;
        Ok(Self { memory })
    }

    pub fn memory(&self) -> &AgentSessionMemory {
        &self.memory
    }

    pub fn memory_mut(&mut self) -> &mut AgentSessionMemory {
        &mut self.memory
    }

    pub fn into_memory(self) -> AgentSessionMemory {
        self.memory
    }

    pub async fn create_session(
        &mut self,
        session_id: Option<String>,
        initial_fields: serde_json::Value,
    ) -> Result<String> {
        self.memory.create_session(session_id, initial_fields).await
    }

    pub fn register_session_command(
        &mut self,
        command_name: impl Into<String>,
        handler: DeterministicContextCommandHandler,
    ) {
        self.memory.register_session_command(command_name, handler);
    }

    pub fn register_session_command_with_schema(
        &mut self,
        command_name: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicContextCommandHandler,
    ) {
        self.memory
            .register_session_command_with_schema(command_name, payload_schema, handler);
    }

    pub async fn apply_session_command(
        &mut self,
        session_id: &str,
        command_name: &str,
        payload_json: serde_json::Value,
        options: AgentCommandOptions,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        self.memory
            .apply_session_command(session_id, command_name, payload_json, options)
            .await
    }

    pub fn get_session_state(&mut self, session_id: &str) -> Result<PersistState> {
        self.memory.get_session_state(session_id)
    }

    pub fn list_session_states(&self) -> Vec<PersistState> {
        self.memory.list_session_states()
    }

    pub async fn timeline_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<AgentSessionTimelineRecord>> {
        self.memory.timeline_for_session(session_id).await
    }

    pub async fn timeline_for_session_with_query(
        &self,
        session_id: &str,
        query: &AgentTimelineQuery,
    ) -> Result<Vec<AgentSessionTimelineRecord>> {
        self.memory
            .timeline_for_session_with_query(session_id, query)
            .await
    }

    pub async fn replay_envelopes_for_session(
        &self,
        session_id: &str,
        query: &AgentTimelineQuery,
    ) -> Result<Vec<RuntimeCommandEnvelope>> {
        self.memory
            .replay_envelopes_for_session(session_id, query)
            .await
    }

    pub async fn replay_session_with_query(
        &mut self,
        source_session_id: &str,
        query: &AgentTimelineQuery,
        options: AgentReplayRunOptions,
    ) -> Result<AgentReplayRunReport> {
        self.memory
            .replay_session_with_query(source_session_id, query, options)
            .await
    }

    pub async fn incident_forensics_report(
        &mut self,
        session_id: &str,
        query: &AgentTimelineQuery,
    ) -> Result<AgentIncidentForensicsReport> {
        self.memory
            .incident_forensics_report(session_id, query)
            .await
    }

    pub async fn incident_forensics_by_correlation(
        &mut self,
        session_id: &str,
        correlation_id: uuid::Uuid,
    ) -> Result<AgentIncidentForensicsReport> {
        let query = AgentTimelineQuery::new().with_correlation_id(correlation_id);
        self.memory
            .incident_forensics_report(session_id, &query)
            .await
    }

    pub async fn incident_forensics_by_causation(
        &mut self,
        session_id: &str,
        causation_id: uuid::Uuid,
    ) -> Result<AgentIncidentForensicsReport> {
        let query = AgentTimelineQuery::new().with_causation_id(causation_id);
        self.memory
            .incident_forensics_report(session_id, &query)
            .await
    }

    pub fn pending_side_effects(&self, session_id: &str) -> Vec<RuntimeOutboxRecord> {
        self.memory.pending_side_effects(session_id)
    }

    pub async fn mark_side_effect_dispatched(&mut self, outbox_id: &str) -> Result<()> {
        self.memory.mark_side_effect_dispatched(outbox_id).await
    }

    pub fn stats(&self) -> RuntimeStats {
        self.memory.stats()
    }

    pub fn slo_metrics(&self) -> RuntimeSloMetrics {
        self.memory.slo_metrics()
    }
}
