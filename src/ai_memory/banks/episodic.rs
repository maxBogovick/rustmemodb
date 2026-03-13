use crate::core::{DbError, Result};
use crate::{
    DeterministicContextCommandHandler, PersistEntityRuntime, PersistMetadata, PersistState,
    RuntimeCommandEnvelope, RuntimeCommandPayloadSchema, RuntimeEnvelopeApplyResult,
    RuntimeJournalOp, RuntimeJournalRecord, RuntimeOperationalPolicy, RuntimeOutboxRecord,
    RuntimeOutboxStatus, RuntimeSloMetrics, RuntimeStats,
};
use chrono::Utc;
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

/// Runtime bootstrap config for episodic agent session memory.
#[derive(Debug, Clone)]
pub struct AgentSessionRuntimeConfig {
    pub root_dir: PathBuf,
    pub policy: RuntimeOperationalPolicy,
    pub entity_type: String,
    pub table_name: String,
    pub schema_version: u32,
}

impl Default for AgentSessionRuntimeConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("./agent_memory_runtime"),
            policy: RuntimeOperationalPolicy::default(),
            entity_type: "AgentSession".to_string(),
            table_name: "agent_session_memory".to_string(),
            schema_version: 1,
        }
    }
}

impl AgentSessionRuntimeConfig {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            ..Self::default()
        }
    }

    pub fn with_policy(mut self, policy: RuntimeOperationalPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_entity_type(mut self, entity_type: impl Into<String>) -> Self {
        self.entity_type = entity_type.into();
        self
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub fn with_schema_version(mut self, schema_version: u32) -> Self {
        self.schema_version = schema_version.max(1);
        self
    }
}

/// Envelope options for session command execution.
#[derive(Debug, Clone)]
pub struct AgentCommandOptions {
    pub expected_version: Option<u64>,
    pub idempotency_key: Option<String>,
    pub actor_id: Option<String>,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    pub payload_version: Option<u32>,
    pub create_session_if_missing: bool,
}

impl Default for AgentCommandOptions {
    fn default() -> Self {
        Self {
            expected_version: None,
            idempotency_key: None,
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            payload_version: None,
            create_session_if_missing: true,
        }
    }
}

impl AgentCommandOptions {
    pub fn with_expected_version(mut self, expected_version: u64) -> Self {
        self.expected_version = Some(expected_version);
        self
    }

    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    pub fn with_actor_id(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_causation_id(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }

    pub fn with_payload_version(mut self, payload_version: u32) -> Self {
        self.payload_version = Some(payload_version.max(1));
        self
    }

    pub fn with_create_session_if_missing(mut self, enable: bool) -> Self {
        self.create_session_if_missing = enable;
        self
    }
}

/// Journal timeline entry filtered for one session.
#[derive(Debug, Clone)]
pub struct AgentSessionTimelineRecord {
    pub record: RuntimeJournalRecord,
    pub matched_by: String,
}

impl AgentSessionTimelineRecord {
    pub fn command_name(&self) -> Option<&str> {
        match &self.record.op {
            RuntimeJournalOp::Upsert {
                command: Some(command),
                ..
            } => Some(command.command.as_str()),
            RuntimeJournalOp::Upsert { envelope, .. } => {
                envelope.as_ref().map(|env| env.command_name.as_str())
            }
            _ => None,
        }
    }

    pub fn envelope(&self) -> Option<&RuntimeCommandEnvelope> {
        match &self.record.op {
            RuntimeJournalOp::Upsert { envelope, .. } => envelope.as_ref(),
            _ => None,
        }
    }
}

/// Query filter for session timeline and replay extraction.
#[derive(Debug, Clone, Default)]
pub struct AgentTimelineQuery {
    pub from_seq: Option<u64>,
    pub to_seq: Option<u64>,
    pub command_name: Option<String>,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    pub limit: Option<usize>,
    pub descending: bool,
}

impl AgentTimelineQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_from_seq(mut self, seq: u64) -> Self {
        self.from_seq = Some(seq);
        self
    }

    pub fn with_to_seq(mut self, seq: u64) -> Self {
        self.to_seq = Some(seq);
        self
    }

    pub fn with_command_name(mut self, command_name: impl Into<String>) -> Self {
        self.command_name = Some(command_name.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_causation_id(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_descending(mut self, descending: bool) -> Self {
        self.descending = descending;
        self
    }

    fn matches(&self, record: &AgentSessionTimelineRecord) -> bool {
        if let Some(from_seq) = self.from_seq {
            if record.record.seq < from_seq {
                return false;
            }
        }
        if let Some(to_seq) = self.to_seq {
            if record.record.seq > to_seq {
                return false;
            }
        }
        if let Some(command_name) = self.command_name.as_deref() {
            if record.command_name() != Some(command_name) {
                return false;
            }
        }
        if let Some(correlation_id) = self.correlation_id {
            if record.envelope().and_then(|env| env.correlation_id) != Some(correlation_id) {
                return false;
            }
        }
        if let Some(causation_id) = self.causation_id {
            if record.envelope().and_then(|env| env.causation_id) != Some(causation_id) {
                return false;
            }
        }
        true
    }
}

/// Controls how session replay should be executed.
#[derive(Debug, Clone)]
pub struct AgentReplayRunOptions {
    pub target_session_id: Option<String>,
    pub target_initial_fields: Option<serde_json::Value>,
    pub create_target_if_missing: bool,
    pub preserve_idempotency_keys: bool,
    pub preserve_trace_ids: bool,
    pub preserve_expected_version: bool,
    pub actor_id_override: Option<String>,
}

impl Default for AgentReplayRunOptions {
    fn default() -> Self {
        Self {
            target_session_id: None,
            target_initial_fields: None,
            create_target_if_missing: true,
            preserve_idempotency_keys: true,
            preserve_trace_ids: true,
            preserve_expected_version: false,
            actor_id_override: None,
        }
    }
}

impl AgentReplayRunOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_target_session_id(mut self, target_session_id: impl Into<String>) -> Self {
        self.target_session_id = Some(target_session_id.into());
        self
    }

    pub fn with_target_initial_fields(mut self, target_initial_fields: serde_json::Value) -> Self {
        self.target_initial_fields = Some(target_initial_fields);
        self
    }

    pub fn with_create_target_if_missing(mut self, enable: bool) -> Self {
        self.create_target_if_missing = enable;
        self
    }

    pub fn with_preserve_idempotency_keys(mut self, enable: bool) -> Self {
        self.preserve_idempotency_keys = enable;
        self
    }

    pub fn with_preserve_trace_ids(mut self, enable: bool) -> Self {
        self.preserve_trace_ids = enable;
        self
    }

    pub fn with_preserve_expected_version(mut self, enable: bool) -> Self {
        self.preserve_expected_version = enable;
        self
    }

    pub fn with_actor_id_override(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id_override = Some(actor_id.into());
        self
    }
}

/// One applied replay step and its resulting state snapshot metadata.
#[derive(Debug, Clone)]
pub struct AgentReplayStepReport {
    pub source_seq: u64,
    pub envelope_id: Uuid,
    pub command_name: String,
    pub idempotent_replay: bool,
    pub resulting_version: i64,
}

/// Aggregate report for one replay run.
#[derive(Debug, Clone)]
pub struct AgentReplayRunReport {
    pub source_session_id: String,
    pub target_session_id: String,
    pub attempted_steps: usize,
    pub applied_steps: usize,
    pub idempotent_replays: usize,
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub final_state: PersistState,
    pub steps: Vec<AgentReplayStepReport>,
}

/// Summarized incident/forensics report for one session and query filter.
#[derive(Debug, Clone)]
pub struct AgentIncidentForensicsReport {
    pub session_id: String,
    pub timeline_records: usize,
    pub replayable_commands: usize,
    pub latest_seq: Option<u64>,
    pub latest_command: Option<String>,
    pub pending_side_effects: usize,
    pub dispatched_side_effects: usize,
    pub state_version: Option<i64>,
    pub matched_by_counts: BTreeMap<String, usize>,
}

/// Episodic memory bank for agent sessions.
pub struct AgentSessionMemory {
    runtime: PersistEntityRuntime,
    entity_type: String,
    table_name: String,
    schema_version: u32,
}

impl AgentSessionMemory {
    pub async fn open(config: AgentSessionRuntimeConfig) -> Result<Self> {
        let runtime = PersistEntityRuntime::open(config.root_dir, config.policy).await?;
        Ok(Self {
            runtime,
            entity_type: config.entity_type,
            table_name: config.table_name,
            schema_version: config.schema_version.max(1),
        })
    }

    pub fn runtime(&self) -> &PersistEntityRuntime {
        &self.runtime
    }

    pub fn runtime_mut(&mut self) -> &mut PersistEntityRuntime {
        &mut self.runtime
    }

    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub async fn create_session(
        &mut self,
        session_id: Option<String>,
        initial_fields: serde_json::Value,
    ) -> Result<String> {
        if !initial_fields.is_object() {
            return Err(DbError::ExecutionError(
                "Session initial fields must be a JSON object".to_string(),
            ));
        }

        let session_id = session_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        if self.session_exists(session_id.as_str()) {
            return Ok(session_id);
        }

        let now = Utc::now();
        let mut metadata = PersistMetadata::new(now);
        metadata.schema_version = self.schema_version;
        metadata.version = 1;
        metadata.touch_count = 1;
        metadata.persisted = true;

        let state = PersistState {
            persist_id: session_id.clone(),
            type_name: self.entity_type.clone(),
            table_name: self.table_name.clone(),
            metadata,
            fields: initial_fields,
        };

        self.runtime
            .upsert_state(state, "agent_session_init")
            .await?;
        Ok(session_id)
    }

    pub async fn ensure_session(&mut self, session_id: &str) -> Result<PersistState> {
        if !self.session_exists(session_id) {
            self.create_session(Some(session_id.to_string()), json!({}))
                .await?;
        }
        self.get_session_state(session_id)
    }

    pub fn get_session_state(&mut self, session_id: &str) -> Result<PersistState> {
        self.runtime
            .get_state(self.entity_type.as_str(), session_id)
    }

    pub fn list_session_states(&self) -> Vec<PersistState> {
        self.runtime
            .list_states()
            .into_iter()
            .filter(|state| state.type_name == self.entity_type)
            .collect()
    }

    pub fn register_session_command(
        &mut self,
        command_name: impl Into<String>,
        handler: DeterministicContextCommandHandler,
    ) {
        self.runtime.register_deterministic_context_command(
            self.entity_type.clone(),
            command_name,
            handler,
        );
    }

    pub fn register_session_command_with_schema(
        &mut self,
        command_name: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicContextCommandHandler,
    ) {
        self.runtime
            .register_deterministic_context_command_with_schema(
                self.entity_type.clone(),
                command_name,
                payload_schema,
                handler,
            );
    }

    pub async fn apply_session_command(
        &mut self,
        session_id: &str,
        command_name: &str,
        payload_json: serde_json::Value,
        options: AgentCommandOptions,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        if options.create_session_if_missing {
            self.ensure_session(session_id).await?;
        } else if !self.session_exists(session_id) {
            return Err(DbError::ExecutionError(format!(
                "Session not found: {}",
                session_id
            )));
        }

        let mut envelope = RuntimeCommandEnvelope::new(
            self.entity_type.clone(),
            session_id.to_string(),
            command_name.to_string(),
            payload_json,
        );
        if let Some(expected_version) = options.expected_version {
            envelope = envelope.with_expected_version(expected_version);
        }
        if let Some(idempotency_key) = options.idempotency_key {
            envelope = envelope.with_idempotency_key(idempotency_key);
        }
        if let Some(actor_id) = options.actor_id {
            envelope = envelope.with_actor_id(actor_id);
        }
        if let Some(correlation_id) = options.correlation_id {
            envelope = envelope.with_correlation_id(correlation_id);
        }
        if let Some(causation_id) = options.causation_id {
            envelope = envelope.with_causation_id(causation_id);
        }
        if let Some(payload_version) = options.payload_version {
            envelope.payload_version = payload_version.max(1);
        }

        self.runtime.apply_command_envelope(envelope).await
    }

    pub fn all_side_effects_for_session(&self, session_id: &str) -> Vec<RuntimeOutboxRecord> {
        self.runtime
            .list_outbox_records()
            .into_iter()
            .filter(|record| {
                record.entity_type == self.entity_type && record.entity_id.as_str() == session_id
            })
            .collect()
    }

    pub fn pending_side_effects(&self, session_id: &str) -> Vec<RuntimeOutboxRecord> {
        self.runtime
            .list_pending_outbox_records()
            .into_iter()
            .filter(|record| {
                record.entity_type == self.entity_type && record.entity_id.as_str() == session_id
            })
            .collect()
    }

    pub async fn mark_side_effect_dispatched(&mut self, outbox_id: &str) -> Result<()> {
        self.runtime.mark_outbox_dispatched(outbox_id).await
    }

    /// Reads timeline records from runtime journal and keeps entries related to the session.
    pub async fn timeline_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<AgentSessionTimelineRecord>> {
        self.timeline_for_session_with_query(session_id, &AgentTimelineQuery::default())
            .await
    }

    /// Reads timeline records from runtime journal and filters entries related to the session.
    pub async fn timeline_for_session_with_query(
        &self,
        session_id: &str,
        query: &AgentTimelineQuery,
    ) -> Result<Vec<AgentSessionTimelineRecord>> {
        let journal_file = self.runtime.paths().journal_file;
        if !journal_file.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&journal_file)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let mut timeline = Vec::new();
        for (line_index, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            let record = serde_json::from_str::<RuntimeJournalRecord>(line).map_err(|err| {
                DbError::ExecutionError(format!(
                    "Failed to parse runtime journal line {}: {}",
                    line_index + 1,
                    err
                ))
            })?;
            if let Some(matched_by) = self.match_session_record(&record, session_id) {
                let item = AgentSessionTimelineRecord { record, matched_by };
                if query.matches(&item) {
                    timeline.push(item);
                }
            }
        }
        if query.descending {
            timeline.sort_by(|a, b| b.record.seq.cmp(&a.record.seq));
        } else {
            timeline.sort_by(|a, b| a.record.seq.cmp(&b.record.seq));
        }
        if let Some(limit) = query.limit {
            timeline.truncate(limit);
        }
        Ok(timeline)
    }

    /// Returns deduplicated command envelopes for replay/forensics under a timeline filter.
    pub async fn replay_envelopes_for_session(
        &self,
        session_id: &str,
        query: &AgentTimelineQuery,
    ) -> Result<Vec<RuntimeCommandEnvelope>> {
        let timeline = self
            .timeline_for_session_with_query(session_id, query)
            .await?;
        let mut seen = HashSet::new();
        let mut envelopes = Vec::new();

        for item in timeline {
            if let Some(envelope) = item.envelope() {
                if seen.insert(envelope.envelope_id) {
                    envelopes.push(envelope.clone());
                }
            }
        }
        Ok(envelopes)
    }

    /// Replays selected command envelopes from one session into a target session.
    ///
    /// This is the main deterministic replay runner for incident analysis and drift checks.
    pub async fn replay_session_with_query(
        &mut self,
        source_session_id: &str,
        query: &AgentTimelineQuery,
        options: AgentReplayRunOptions,
    ) -> Result<AgentReplayRunReport> {
        let target_session_id = options.target_session_id.clone().unwrap_or_else(|| {
            format!("{}__replay__{}", source_session_id, Uuid::new_v4().simple())
        });

        if options.create_target_if_missing {
            if !self.session_exists(target_session_id.as_str()) {
                let initial_fields = options
                    .target_initial_fields
                    .clone()
                    .unwrap_or_else(|| json!({}));
                self.create_session(Some(target_session_id.clone()), initial_fields)
                    .await?;
            }
        } else if !self.session_exists(target_session_id.as_str()) {
            return Err(DbError::ExecutionError(format!(
                "Replay target session not found: {}",
                target_session_id
            )));
        }

        let replay_steps = self
            .replay_items_for_session(source_session_id, query)
            .await?;
        let mut step_reports = Vec::new();
        let mut idempotent_replays = 0usize;

        for (source_seq, mut envelope) in replay_steps {
            envelope.entity_id = target_session_id.clone();
            if !options.preserve_expected_version {
                envelope.expected_version = None;
            }
            if !options.preserve_idempotency_keys {
                envelope.idempotency_key = None;
            }
            if !options.preserve_trace_ids {
                envelope.correlation_id = None;
                envelope.causation_id = None;
            }
            if let Some(actor_id) = options.actor_id_override.as_ref() {
                envelope.actor_id = Some(actor_id.clone());
            }

            let command_name = envelope.command_name.clone();
            let result = self.runtime.apply_command_envelope(envelope).await?;
            if result.idempotent_replay {
                idempotent_replays = idempotent_replays.saturating_add(1);
            }
            step_reports.push(AgentReplayStepReport {
                source_seq,
                envelope_id: result.envelope_id,
                command_name,
                idempotent_replay: result.idempotent_replay,
                resulting_version: result.state.metadata.version,
            });
        }

        let final_state = self.get_session_state(target_session_id.as_str())?;
        let first_seq = step_reports.first().map(|step| step.source_seq);
        let last_seq = step_reports.last().map(|step| step.source_seq);

        Ok(AgentReplayRunReport {
            source_session_id: source_session_id.to_string(),
            target_session_id,
            attempted_steps: step_reports.len(),
            applied_steps: step_reports.len(),
            idempotent_replays,
            first_seq,
            last_seq,
            final_state,
            steps: step_reports,
        })
    }

    /// Returns condensed incident/forensics summary for the selected timeline segment.
    pub async fn incident_forensics_report(
        &mut self,
        session_id: &str,
        query: &AgentTimelineQuery,
    ) -> Result<AgentIncidentForensicsReport> {
        let timeline = self
            .timeline_for_session_with_query(session_id, query)
            .await?;
        let mut matched_by_counts: BTreeMap<String, usize> = BTreeMap::new();
        let mut replayable_commands = 0usize;
        let mut latest_seq = None;
        let mut latest_command = None;

        for item in &timeline {
            let entry = matched_by_counts
                .entry(item.matched_by.clone())
                .or_insert(0usize);
            *entry = entry.saturating_add(1);

            if item.envelope().is_some() {
                replayable_commands = replayable_commands.saturating_add(1);
            }
            if latest_seq.map(|seq| item.record.seq > seq).unwrap_or(true) {
                latest_seq = Some(item.record.seq);
                latest_command = item.command_name().map(str::to_string);
            }
        }

        let all_side_effects = self.all_side_effects_for_session(session_id);
        let pending_side_effects = all_side_effects
            .iter()
            .filter(|record| record.status == RuntimeOutboxStatus::Pending)
            .count();
        let dispatched_side_effects = all_side_effects
            .iter()
            .filter(|record| record.status == RuntimeOutboxStatus::Dispatched)
            .count();

        let state_version = self
            .get_session_state(session_id)
            .ok()
            .map(|state| state.metadata.version);

        Ok(AgentIncidentForensicsReport {
            session_id: session_id.to_string(),
            timeline_records: timeline.len(),
            replayable_commands,
            latest_seq,
            latest_command,
            pending_side_effects,
            dispatched_side_effects,
            state_version,
            matched_by_counts,
        })
    }

    pub fn stats(&self) -> RuntimeStats {
        self.runtime.stats()
    }

    pub fn slo_metrics(&self) -> RuntimeSloMetrics {
        self.runtime.slo_metrics()
    }

    fn session_exists(&self, session_id: &str) -> bool {
        self.runtime.list_states().into_iter().any(|state| {
            state.type_name == self.entity_type && state.persist_id.as_str() == session_id
        })
    }

    async fn replay_items_for_session(
        &self,
        session_id: &str,
        query: &AgentTimelineQuery,
    ) -> Result<Vec<(u64, RuntimeCommandEnvelope)>> {
        let timeline = self
            .timeline_for_session_with_query(session_id, query)
            .await?;
        let mut replay_steps = Vec::new();
        let mut seen = HashSet::new();

        for item in timeline {
            if let Some(envelope) = item.envelope() {
                if seen.insert(envelope.envelope_id) {
                    replay_steps.push((item.record.seq, envelope.clone()));
                }
            }
        }
        replay_steps.sort_by_key(|(seq, _)| *seq);
        Ok(replay_steps)
    }

    fn match_session_record(
        &self,
        record: &RuntimeJournalRecord,
        session_id: &str,
    ) -> Option<String> {
        match &record.op {
            RuntimeJournalOp::Upsert {
                entity, envelope, ..
            } => {
                if entity.state.type_name == self.entity_type
                    && entity.state.persist_id.as_str() == session_id
                {
                    return Some("upsert_entity_state".to_string());
                }
                if envelope.as_ref().is_some_and(|env| {
                    env.entity_type == self.entity_type && env.entity_id.as_str() == session_id
                }) {
                    return Some("upsert_envelope".to_string());
                }
                None
            }
            RuntimeJournalOp::Delete { key, .. } => {
                if key.entity_type == self.entity_type && key.persist_id.as_str() == session_id {
                    Some("delete".to_string())
                } else {
                    None
                }
            }
            RuntimeJournalOp::OutboxUpsert { record } => {
                if record.entity_type == self.entity_type && record.entity_id.as_str() == session_id
                {
                    Some("outbox_upsert".to_string())
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentCommandOptions, AgentReplayRunOptions, AgentSessionMemory, AgentSessionRuntimeConfig,
        AgentTimelineQuery,
    };
    use crate::ai_memory::runtime::agent_workflow::{AgentWorkflowExecutor, AgentWorkflowStep};
    use crate::{
        DeterministicContextCommandHandler, Result, RuntimeCommandPayloadSchema, RuntimeJournalOp,
        RuntimePayloadType, RuntimeSideEffectSpec,
    };
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_runtime_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rustmemodb_ai_memory_{}_{}", name, Uuid::new_v4()))
    }

    #[tokio::test]
    async fn episodic_session_command_supports_idempotent_replay_and_outbox() -> Result<()> {
        let root = test_runtime_root("episodic_idempotency");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        let handler: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let fields = state.fields_object_mut()?;
            let current = fields.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            fields.insert("count".to_string(), json!(current + delta));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(vec![RuntimeSideEffectSpec {
                effect_type: "memory.incremented".to_string(),
                payload_json: json!({ "count": current + delta }),
            }])
        });
        memory.register_session_command_with_schema(
            "increment",
            RuntimeCommandPayloadSchema::object()
                .require_field("delta", RuntimePayloadType::Integer)
                .allow_extra_fields(false),
            handler,
        );

        let session_id = memory.create_session(None, json!({ "count": 0 })).await?;
        let options = AgentCommandOptions::default()
            .with_idempotency_key("step-001")
            .with_actor_id("agent:test");

        let first = memory
            .apply_session_command(
                &session_id,
                "increment",
                json!({ "delta": 2 }),
                options.clone(),
            )
            .await?;
        let second = memory
            .apply_session_command(&session_id, "increment", json!({ "delta": 2 }), options)
            .await?;

        assert!(!first.idempotent_replay);
        assert!(second.idempotent_replay);
        assert_eq!(
            second
                .state
                .fields_object()?
                .get("count")
                .and_then(|v| v.as_i64()),
            Some(2)
        );

        let pending = memory.pending_side_effects(&session_id);
        assert_eq!(pending.len(), 1);
        memory
            .mark_side_effect_dispatched(pending[0].outbox_id.as_str())
            .await?;
        assert!(memory.pending_side_effects(&session_id).is_empty());

        let timeline = memory.timeline_for_session(&session_id).await?;
        assert!(!timeline.is_empty());
        assert!(timeline.iter().any(|item| {
            matches!(
                &item.record.op,
                RuntimeJournalOp::Upsert {
                    command: Some(command),
                    ..
                } if command.command == "increment"
            )
        }));

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn workflow_executor_runs_steps_on_same_session() -> Result<()> {
        let root = test_runtime_root("workflow");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        let set_stage: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let stage = payload
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let fields = state.fields_object_mut()?;
            fields.insert("stage".to_string(), json!(stage));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        });
        memory.register_session_command_with_schema(
            "set_stage",
            RuntimeCommandPayloadSchema::object()
                .require_field("stage", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            set_stage,
        );

        let session_id = memory
            .create_session(None, json!({ "stage": "new" }))
            .await?;
        let executor = AgentWorkflowExecutor::new(&mut memory, session_id.as_str());
        let results = executor
            .run(vec![
                AgentWorkflowStep::new("set_stage", json!({ "stage": "planning" })),
                AgentWorkflowStep::new("set_stage", json!({ "stage": "running" })),
            ])
            .await?;

        assert_eq!(results.len(), 2);
        let state = memory.get_session_state(session_id.as_str())?;
        assert_eq!(
            state.fields_object()?.get("stage").and_then(|v| v.as_str()),
            Some("running")
        );

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn timeline_query_filters_by_command_and_correlation() -> Result<()> {
        let root = test_runtime_root("timeline_query");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        let set_stage: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let stage = payload
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let fields = state.fields_object_mut()?;
            fields.insert("stage".to_string(), json!(stage));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        });
        memory.register_session_command_with_schema(
            "set_stage",
            RuntimeCommandPayloadSchema::object()
                .require_field("stage", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            set_stage,
        );

        let session_id = memory
            .create_session(None, json!({ "stage": "new" }))
            .await?;
        let corr_a = Uuid::new_v4();
        let corr_b = Uuid::new_v4();

        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "planning" }),
                AgentCommandOptions::default().with_correlation_id(corr_a),
            )
            .await?;
        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "running" }),
                AgentCommandOptions::default().with_correlation_id(corr_b),
            )
            .await?;

        let query = AgentTimelineQuery::new()
            .with_command_name("set_stage")
            .with_correlation_id(corr_a);
        let timeline = memory
            .timeline_for_session_with_query(session_id.as_str(), &query)
            .await?;
        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].command_name(), Some("set_stage"));
        assert_eq!(
            timeline[0].envelope().and_then(|env| env.correlation_id),
            Some(corr_a)
        );

        let replay = memory
            .replay_envelopes_for_session(session_id.as_str(), &query)
            .await?;
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].command_name.as_str(), "set_stage");
        assert_eq!(replay[0].correlation_id, Some(corr_a));

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn workflow_executor_can_apply_generated_shared_correlation() -> Result<()> {
        let root = test_runtime_root("workflow_shared_correlation");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        let set_stage: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let stage = payload
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let fields = state.fields_object_mut()?;
            fields.insert("stage".to_string(), json!(stage));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        });
        memory.register_session_command_with_schema(
            "set_stage",
            RuntimeCommandPayloadSchema::object()
                .require_field("stage", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            set_stage,
        );

        let session_id = memory
            .create_session(None, json!({ "stage": "new" }))
            .await?;

        let executor = AgentWorkflowExecutor::new(&mut memory, session_id.as_str());
        let (correlation_id, results) = executor
            .run_with_generated_correlation(vec![
                AgentWorkflowStep::new("set_stage", json!({ "stage": "planning" })),
                AgentWorkflowStep::new("set_stage", json!({ "stage": "running" })),
            ])
            .await?;

        assert_eq!(results.len(), 2);
        let query = AgentTimelineQuery::new()
            .with_command_name("set_stage")
            .with_correlation_id(correlation_id);
        let replay = memory
            .replay_envelopes_for_session(session_id.as_str(), &query)
            .await?;
        assert_eq!(replay.len(), 2);
        assert!(
            replay
                .iter()
                .all(|envelope| envelope.correlation_id == Some(correlation_id))
        );

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn timeline_query_supports_seq_range_desc_and_limit() -> Result<()> {
        let root = test_runtime_root("timeline_seq_order_limit");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        let set_stage: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let stage = payload
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let fields = state.fields_object_mut()?;
            fields.insert("stage".to_string(), json!(stage));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        });
        memory.register_session_command_with_schema(
            "set_stage",
            RuntimeCommandPayloadSchema::object()
                .require_field("stage", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            set_stage,
        );

        let session_id = memory
            .create_session(None, json!({ "stage": "new" }))
            .await?;
        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "planning" }),
                AgentCommandOptions::default(),
            )
            .await?;
        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "running" }),
                AgentCommandOptions::default(),
            )
            .await?;
        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "done" }),
                AgentCommandOptions::default(),
            )
            .await?;

        let all_commands = memory
            .timeline_for_session_with_query(
                session_id.as_str(),
                &AgentTimelineQuery::new().with_command_name("set_stage"),
            )
            .await?;
        assert_eq!(all_commands.len(), 3);
        let second_seq = all_commands[1].record.seq;
        let third_seq = all_commands[2].record.seq;

        let filtered = memory
            .timeline_for_session_with_query(
                session_id.as_str(),
                &AgentTimelineQuery::new()
                    .with_command_name("set_stage")
                    .with_from_seq(second_seq)
                    .with_descending(true)
                    .with_limit(1),
            )
            .await?;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].record.seq, third_seq);

        let bounded = memory
            .timeline_for_session_with_query(
                session_id.as_str(),
                &AgentTimelineQuery::new()
                    .with_command_name("set_stage")
                    .with_from_seq(second_seq)
                    .with_to_seq(second_seq),
            )
            .await?;
        assert_eq!(bounded.len(), 1);
        assert_eq!(bounded[0].record.seq, second_seq);

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn workflow_shared_correlation_preserves_explicit_step_override() -> Result<()> {
        let root = test_runtime_root("workflow_correlation_override");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        let set_stage: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let stage = payload
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let fields = state.fields_object_mut()?;
            fields.insert("stage".to_string(), json!(stage));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        });
        memory.register_session_command_with_schema(
            "set_stage",
            RuntimeCommandPayloadSchema::object()
                .require_field("stage", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            set_stage,
        );

        let session_id = memory
            .create_session(None, json!({ "stage": "new" }))
            .await?;

        let shared_correlation = Uuid::new_v4();
        let explicit_correlation = Uuid::new_v4();
        let executor = AgentWorkflowExecutor::new(&mut memory, session_id.as_str());
        let results = executor
            .run_with_correlation(
                vec![
                    AgentWorkflowStep::new("set_stage", json!({ "stage": "planning" })),
                    AgentWorkflowStep::new("set_stage", json!({ "stage": "running" }))
                        .with_options(
                            AgentCommandOptions::default()
                                .with_correlation_id(explicit_correlation),
                        ),
                ],
                shared_correlation,
            )
            .await?;

        assert_eq!(results.len(), 2);

        let shared_replay = memory
            .replay_envelopes_for_session(
                session_id.as_str(),
                &AgentTimelineQuery::new()
                    .with_command_name("set_stage")
                    .with_correlation_id(shared_correlation),
            )
            .await?;
        let explicit_replay = memory
            .replay_envelopes_for_session(
                session_id.as_str(),
                &AgentTimelineQuery::new()
                    .with_command_name("set_stage")
                    .with_correlation_id(explicit_correlation),
            )
            .await?;
        assert_eq!(shared_replay.len(), 1);
        assert_eq!(explicit_replay.len(), 1);
        assert_eq!(shared_replay[0].correlation_id, Some(shared_correlation));
        assert_eq!(
            explicit_replay[0].correlation_id,
            Some(explicit_correlation)
        );

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn apply_session_command_respects_create_if_missing_false() -> Result<()> {
        let root = test_runtime_root("strict_missing_session");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        let noop: DeterministicContextCommandHandler = Arc::new(|state, _payload, _ctx| {
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        });
        memory.register_session_command("noop", noop);

        let result = memory
            .apply_session_command(
                "missing-session",
                "noop",
                json!({}),
                AgentCommandOptions::default().with_create_session_if_missing(false),
            )
            .await;

        assert!(result.is_err());
        let error_text = match result {
            Err(error) => error.to_string(),
            Ok(_) => String::new(),
        };
        assert!(error_text.contains("Session not found"));

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn incident_forensics_report_summarizes_filtered_workflow() -> Result<()> {
        // GOAL: prove that incident report summarizes filtered command timeline and side-effect status.
        // DEBUG EXPECT: timeline for correlation A contains exactly 2 command envelopes.
        let root = test_runtime_root("incident_forensics_summary");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        // GOAL: register deterministic command that emits one side-effect per command.
        // DEBUG EXPECT: each successful command adds one outbox record with effect_type stage.changed.
        let set_stage: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let stage = payload
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let fields = state.fields_object_mut()?;
            fields.insert("stage".to_string(), json!(stage));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(vec![RuntimeSideEffectSpec {
                effect_type: "stage.changed".to_string(),
                payload_json: json!({ "stage": stage }),
            }])
        });
        memory.register_session_command_with_schema(
            "set_stage",
            RuntimeCommandPayloadSchema::object()
                .require_field("stage", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            set_stage,
        );

        // GOAL: create baseline session state.
        // DEBUG EXPECT: initial state version = 1 and stage = new.
        let session_id = memory
            .create_session(None, json!({ "stage": "new" }))
            .await?;

        // GOAL: create two correlated commands (corr_a) and one independent command (corr_b).
        // DEBUG EXPECT: corr_a => 2 commands, corr_b => 1 command.
        let corr_a = Uuid::new_v4();
        let corr_b = Uuid::new_v4();
        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "planning" }),
                AgentCommandOptions::default()
                    .with_correlation_id(corr_a)
                    .with_idempotency_key("a-1"),
            )
            .await?;
        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "running" }),
                AgentCommandOptions::default()
                    .with_correlation_id(corr_a)
                    .with_idempotency_key("a-2"),
            )
            .await?;
        memory
            .apply_session_command(
                session_id.as_str(),
                "set_stage",
                json!({ "stage": "done" }),
                AgentCommandOptions::default()
                    .with_correlation_id(corr_b)
                    .with_idempotency_key("b-1"),
            )
            .await?;

        // GOAL: mark exactly one side-effect as dispatched so report sees pending + dispatched split.
        // DEBUG EXPECT before dispatch: pending = 3; after dispatch: pending = 2, dispatched = 1.
        let pending_before_dispatch = memory.pending_side_effects(session_id.as_str());
        assert_eq!(pending_before_dispatch.len(), 3);
        memory
            .mark_side_effect_dispatched(pending_before_dispatch[0].outbox_id.as_str())
            .await?;

        // GOAL: request incident summary only for correlation A command timeline.
        // DEBUG EXPECT: timeline_records = 2 and replayable_commands = 2.
        let query = AgentTimelineQuery::new()
            .with_command_name("set_stage")
            .with_correlation_id(corr_a);
        let report = memory
            .incident_forensics_report(session_id.as_str(), &query)
            .await?;

        // GOAL: validate filtered timeline summary.
        // DEBUG EXPECT: only correlation A commands are counted in timeline stats.
        assert_eq!(report.timeline_records, 2);
        assert_eq!(report.replayable_commands, 2);
        assert_eq!(report.latest_command.as_deref(), Some("set_stage"));

        // GOAL: validate side-effect operational summary.
        // DEBUG EXPECT: pending = 2, dispatched = 1 after one explicit dispatch mark.
        assert_eq!(report.pending_side_effects, 2);
        assert_eq!(report.dispatched_side_effects, 1);

        // GOAL: validate that match source buckets are tracked for explainability.
        // DEBUG EXPECT: upsert_entity_state bucket exists for session command upserts.
        let upsert_count = report
            .matched_by_counts
            .get("upsert_entity_state")
            .copied()
            .unwrap_or(0);
        assert_eq!(upsert_count, 2);

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn replay_runner_recovers_state_after_runtime_restart_without_drift() -> Result<()> {
        // GOAL: prove replay works after process restart and reproduces source state without drift.
        // DEBUG EXPECT: source.count and mirror.count must match exactly.
        let root = test_runtime_root("replay_restart_no_drift");
        let config = AgentSessionRuntimeConfig::new(&root);

        // GOAL: build source history in first runtime instance.
        // DEBUG EXPECT: source session is persisted to disk and available after reopen.
        let (session_id, source_fields_snapshot, source_version_snapshot) = {
            let mut memory = AgentSessionMemory::open(config.clone()).await?;

            let increment: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
                let delta = payload
                    .get("delta")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(1);
                let fields = state.fields_object_mut()?;
                let current = fields
                    .get("count")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                fields.insert("count".to_string(), json!(current + delta));
                state.metadata.version = state.metadata.version.saturating_add(1);
                Ok(Vec::new())
            });
            memory.register_session_command_with_schema(
                "increment",
                RuntimeCommandPayloadSchema::object()
                    .require_field("delta", RuntimePayloadType::Integer)
                    .allow_extra_fields(false),
                increment,
            );

            // GOAL: initialize known baseline for deterministic replay.
            // DEBUG EXPECT: initial count = 0.
            let session_id = memory.create_session(None, json!({ "count": 0 })).await?;

            // GOAL: apply 3 commands to build realistic command timeline.
            // DEBUG EXPECT: final count = 6.
            memory
                .apply_session_command(
                    session_id.as_str(),
                    "increment",
                    json!({ "delta": 1 }),
                    AgentCommandOptions::default()
                        .with_idempotency_key("inc-1")
                        .with_correlation_id(Uuid::new_v4()),
                )
                .await?;
            memory
                .apply_session_command(
                    session_id.as_str(),
                    "increment",
                    json!({ "delta": 2 }),
                    AgentCommandOptions::default()
                        .with_idempotency_key("inc-2")
                        .with_correlation_id(Uuid::new_v4()),
                )
                .await?;
            memory
                .apply_session_command(
                    session_id.as_str(),
                    "increment",
                    json!({ "delta": 3 }),
                    AgentCommandOptions::default()
                        .with_idempotency_key("inc-3")
                        .with_correlation_id(Uuid::new_v4()),
                )
                .await?;

            // GOAL: capture source snapshot for no-drift assertion after replay.
            // DEBUG EXPECT: source fields include count = 6.
            let source_state = memory.get_session_state(session_id.as_str())?;
            (
                session_id,
                source_state.fields.clone(),
                source_state.metadata.version,
            )
        };

        // GOAL: reopen runtime (simulates service restart) and run replay into mirror session.
        // DEBUG EXPECT: mirror replay runs after reopen without losing source timeline.
        let mut memory = AgentSessionMemory::open(config).await?;
        let increment: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let delta = payload
                .get("delta")
                .and_then(|value| value.as_i64())
                .unwrap_or(1);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|value| value.as_i64())
                .unwrap_or(0);
            fields.insert("count".to_string(), json!(current + delta));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        });
        memory.register_session_command_with_schema(
            "increment",
            RuntimeCommandPayloadSchema::object()
                .require_field("delta", RuntimePayloadType::Integer)
                .allow_extra_fields(false),
            increment,
        );

        // GOAL: replay all increment commands into fresh mirror state.
        // DEBUG EXPECT: attempted_steps = 3 and mirror final count = source final count.
        let replay_report = memory
            .replay_session_with_query(
                session_id.as_str(),
                &AgentTimelineQuery::new().with_command_name("increment"),
                AgentReplayRunOptions::new()
                    .with_target_session_id("mirror-session")
                    .with_target_initial_fields(json!({ "count": 0 })),
            )
            .await?;
        assert_eq!(replay_report.attempted_steps, 3);

        // GOAL: strict drift-check between source snapshot and mirror state.
        // DEBUG EXPECT: fields and version are exactly equal after deterministic replay.
        let mirror_state = memory.get_session_state("mirror-session")?;
        assert_eq!(mirror_state.fields, source_fields_snapshot);
        assert_eq!(mirror_state.metadata.version, source_version_snapshot);

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }

    #[tokio::test]
    async fn replay_runner_with_preserved_idempotency_does_not_duplicate_side_effects() -> Result<()>
    {
        // GOAL: prove replay on same session keeps idempotency guarantees and avoids double side-effects.
        // DEBUG EXPECT: side-effect count remains stable after replay runner executes.
        let root = test_runtime_root("replay_idempotency_no_duplicate_effects");
        let config = AgentSessionRuntimeConfig::new(&root);
        let mut memory = AgentSessionMemory::open(config).await?;

        // GOAL: command mutates counter and emits one outbox side-effect.
        // DEBUG EXPECT: first command => exactly one pending outbox record.
        let increment: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
            let delta = payload
                .get("delta")
                .and_then(|value| value.as_i64())
                .unwrap_or(1);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|value| value.as_i64())
                .unwrap_or(0);
            fields.insert("count".to_string(), json!(current + delta));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(vec![RuntimeSideEffectSpec {
                effect_type: "counter.incremented".to_string(),
                payload_json: json!({ "count": current + delta }),
            }])
        });
        memory.register_session_command_with_schema(
            "increment",
            RuntimeCommandPayloadSchema::object()
                .require_field("delta", RuntimePayloadType::Integer)
                .allow_extra_fields(false),
            increment,
        );

        // GOAL: establish source event with stable idempotency key.
        // DEBUG EXPECT: command is non-replay on first call.
        let session_id = memory.create_session(None, json!({ "count": 0 })).await?;
        let first_apply = memory
            .apply_session_command(
                session_id.as_str(),
                "increment",
                json!({ "delta": 2 }),
                AgentCommandOptions::default().with_idempotency_key("inc-stable-key"),
            )
            .await?;
        assert!(!first_apply.idempotent_replay);

        // GOAL: capture side-effect baseline before replay.
        // DEBUG EXPECT: pending = 1.
        let pending_before = memory.pending_side_effects(session_id.as_str());
        assert_eq!(pending_before.len(), 1);

        // GOAL: replay the same command envelope back into the SAME session.
        // DEBUG EXPECT: replay runner reports idempotent replay and creates no extra outbox rows.
        let replay_report = memory
            .replay_session_with_query(
                session_id.as_str(),
                &AgentTimelineQuery::new().with_command_name("increment"),
                AgentReplayRunOptions::new().with_target_session_id(session_id.as_str()),
            )
            .await?;
        assert_eq!(replay_report.applied_steps, 1);
        assert_eq!(replay_report.idempotent_replays, 1);

        // GOAL: verify side-effects are not duplicated by replay.
        // DEBUG EXPECT: total side-effects stays exactly one.
        let all_after = memory.all_side_effects_for_session(session_id.as_str());
        assert_eq!(all_after.len(), 1);
        let pending_after = memory.pending_side_effects(session_id.as_str());
        assert_eq!(pending_after.len(), 1);

        let _ = tokio::fs::remove_dir_all(&root).await;
        Ok(())
    }
}
