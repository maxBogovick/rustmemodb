use crate::RuntimeEnvelopeApplyResult;
use crate::ai_memory::banks::episodic::{AgentCommandOptions, AgentSessionMemory};
use crate::core::Result;
use uuid::Uuid;

/// One deterministic command step executed inside a session workflow.
#[derive(Debug, Clone)]
pub struct AgentWorkflowStep {
    pub command_name: String,
    pub payload_json: serde_json::Value,
    pub options: AgentCommandOptions,
}

impl AgentWorkflowStep {
    pub fn new(command_name: impl Into<String>, payload_json: serde_json::Value) -> Self {
        Self {
            command_name: command_name.into(),
            payload_json,
            options: AgentCommandOptions::default(),
        }
    }

    pub fn with_options(mut self, options: AgentCommandOptions) -> Self {
        self.options = options;
        self
    }
}

/// Minimal workflow executor for sequential deterministic session commands.
pub struct AgentWorkflowExecutor<'a> {
    memory: &'a mut AgentSessionMemory,
    session_id: String,
}

impl<'a> AgentWorkflowExecutor<'a> {
    pub fn new(memory: &'a mut AgentSessionMemory, session_id: impl Into<String>) -> Self {
        Self {
            memory,
            session_id: session_id.into(),
        }
    }

    pub async fn run<I>(self, steps: I) -> Result<Vec<RuntimeEnvelopeApplyResult>>
    where
        I: IntoIterator<Item = AgentWorkflowStep>,
    {
        self.run_internal(steps, None).await
    }

    /// Runs steps under a shared correlation ID unless a step already sets one explicitly.
    pub async fn run_with_correlation<I>(
        self,
        steps: I,
        correlation_id: Uuid,
    ) -> Result<Vec<RuntimeEnvelopeApplyResult>>
    where
        I: IntoIterator<Item = AgentWorkflowStep>,
    {
        self.run_internal(steps, Some(correlation_id)).await
    }

    /// Generates a new workflow correlation ID and applies it to all steps by default.
    pub async fn run_with_generated_correlation<I>(
        self,
        steps: I,
    ) -> Result<(Uuid, Vec<RuntimeEnvelopeApplyResult>)>
    where
        I: IntoIterator<Item = AgentWorkflowStep>,
    {
        let correlation_id = Uuid::new_v4();
        let results = self.run_internal(steps, Some(correlation_id)).await?;
        Ok((correlation_id, results))
    }

    async fn run_internal<I>(
        self,
        steps: I,
        default_correlation: Option<Uuid>,
    ) -> Result<Vec<RuntimeEnvelopeApplyResult>>
    where
        I: IntoIterator<Item = AgentWorkflowStep>,
    {
        let AgentWorkflowExecutor { memory, session_id } = self;
        let mut results = Vec::new();
        for mut step in steps {
            if step.options.correlation_id.is_none() {
                if let Some(correlation_id) = default_correlation {
                    step.options = step.options.with_correlation_id(correlation_id);
                }
            }
            let result = memory
                .apply_session_command(
                    session_id.as_str(),
                    step.command_name.as_str(),
                    step.payload_json,
                    step.options,
                )
                .await?;
            results.push(result);
        }
        Ok(results)
    }
}
