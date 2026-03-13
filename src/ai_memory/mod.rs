//! High-level AI memory surface built on top of `PersistEntityRuntime`.
//!
//! Phase 1 focuses on episodic memory for agent sessions:
//! - typed session bootstrap,
//! - deterministic command envelopes,
//! - idempotency/outbox aware execution,
//! - lightweight timeline forensics,
//! - incident replay runner.

pub mod banks;
pub mod runtime;

pub use banks::episodic::{
    AgentCommandOptions, AgentIncidentForensicsReport, AgentReplayRunOptions, AgentReplayRunReport,
    AgentReplayStepReport, AgentSessionMemory, AgentSessionRuntimeConfig,
    AgentSessionTimelineRecord, AgentTimelineQuery,
};
pub use runtime::agent_session::AgentSessionRuntime;
pub use runtime::agent_workflow::{AgentWorkflowExecutor, AgentWorkflowStep};
