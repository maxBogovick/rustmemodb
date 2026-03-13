use chrono::{DateTime, Utc};
use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

const WORKSPACE_NAME_MAX_LEN: usize = 80;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct AgentProfile {
    pub id: String,
    pub handle: String,
    pub model: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
#[serde(rename_all = "snake_case")]
pub enum MissionStatus {
    Draft,
    Active,
    Paused,
    Archived,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct Mission {
    pub id: String,
    pub title: String,
    pub objective: String,
    pub priority: u8,
    pub status: MissionStatus,
    pub owner_agent_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    WaitingHandoff,
    Succeeded,
    Failed,
    Canceled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct RunStep {
    pub id: String,
    pub phase: String,
    pub summary: String,
    pub latency_ms: u64,
    pub token_cost: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct MissionRun {
    pub id: String,
    pub mission_id: String,
    pub assigned_agent_id: String,
    pub state: RunState,
    pub attempt: u32,
    pub steps: Vec<RunStep>,
    pub token_cost_total: i64,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
#[serde(rename_all = "snake_case")]
pub enum IncidentSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Open,
    Mitigated,
    Resolved,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct IncidentRecord {
    pub id: String,
    pub run_id: Option<String>,
    pub severity: IncidentSeverity,
    pub status: IncidentStatus,
    pub title: String,
    pub details: String,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolution_note: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistView)]
#[persist_view(
    model = AgentOpsWorkspace,
    name = "ops_dashboard",
    compute = compute_ops_dashboard_view
)]
pub struct OpsDashboardView {
    pub workspace_name: String,
    pub agents_total: usize,
    pub active_agents: usize,
    pub missions_active: usize,
    pub runs_running: usize,
    pub incidents_open: usize,
    pub token_cost_total: i64,
    pub slow_steps_over_2s: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistView)]
#[persist_view(model = AgentOpsWorkspace, name = "reliability")]
pub struct ReliabilityView {
    #[view_metric(kind = "copy", source = "name")]
    pub workspace_name: String,
    #[view_metric(kind = "count", source = "missions")]
    pub missions_total: i64,
    #[view_metric(kind = "count", source = "runs")]
    pub runs_total: i64,
    #[view_metric(kind = "count", source = "incidents")]
    pub incidents_total: i64,
    #[view_metric(kind = "sum", source = "runs", field = "token_cost_total")]
    pub token_cost_total: i64,
    #[view_metric(kind = "group_by", source = "runs", by = "state", op = "count")]
    pub runs_by_state: BTreeMap<String, i64>,
    #[view_metric(kind = "group_by", source = "incidents", by = "severity", op = "count")]
    pub incidents_by_severity: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MissionHealthSnapshot {
    pub mission_id: String,
    pub title: String,
    pub status: MissionStatus,
    pub total_runs: usize,
    pub successful_runs: usize,
    pub failed_runs: usize,
    pub avg_step_latency_ms: Option<f64>,
    pub last_error: Option<String>,
    pub run_state_breakdown: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentLoadSnapshot {
    pub agent_id: String,
    pub handle: String,
    pub active: bool,
    pub running_runs: usize,
    pub waiting_handoff_runs: usize,
    pub assigned_active_missions: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct RegisterAgentInput {
    #[validate(trim, non_empty, len_max = 64)]
    pub handle: String,
    #[validate(trim, non_empty, len_max = 64)]
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct RenameWorkspaceInput {
    #[validate(trim, non_empty, len_max = 80)]
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct SetAgentActiveInput {
    #[validate(trim, non_empty)]
    pub agent_id: String,
    pub active: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct CreateMissionInput {
    #[validate(trim, non_empty, len_max = 120)]
    pub title: String,
    #[validate(trim, non_empty, len_max = 800)]
    pub objective: String,
    #[validate(min = 1, max = 5)]
    pub priority: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct ActivateMissionInput {
    #[validate(trim, non_empty)]
    pub mission_id: String,
    #[validate(trim, non_empty)]
    pub owner_agent_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct MissionIdInput {
    #[validate(trim, non_empty)]
    pub mission_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct StartRunInput {
    #[validate(trim, non_empty)]
    pub mission_id: String,
    #[validate(trim, non_empty)]
    pub assigned_agent_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct AppendRunStepInput {
    #[validate(trim, non_empty)]
    pub run_id: String,
    #[validate(trim, non_empty, len_max = 48)]
    pub phase: String,
    #[validate(trim, non_empty, len_max = 512)]
    pub summary: String,
    pub latency_ms: u64,
    #[validate(min = 0)]
    pub token_cost: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct HandoffRunInput {
    #[validate(trim, non_empty)]
    pub run_id: String,
    #[validate(trim, non_empty)]
    pub to_agent_id: String,
    #[validate(trim, non_empty, len_max = 512)]
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct RunIdInput {
    #[validate(trim, non_empty)]
    pub run_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct FailRunInput {
    #[validate(trim, non_empty)]
    pub run_id: String,
    #[validate(trim, non_empty, len_max = 512)]
    pub error_message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct CancelRunInput {
    #[validate(trim, non_empty)]
    pub run_id: String,
    #[validate(trim, non_empty, len_max = 512)]
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct RetryRunInput {
    #[validate(trim, non_empty)]
    pub run_id: String,
    #[validate(trim, non_empty)]
    pub assigned_agent_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct RaiseIncidentInput {
    #[validate(trim, non_empty)]
    pub run_id: Option<String>,
    pub severity: IncidentSeverity,
    #[validate(trim, non_empty, len_max = 140)]
    pub title: String,
    #[validate(trim, non_empty, len_max = 1500)]
    pub details: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct ResolveIncidentInput {
    #[validate(trim, non_empty)]
    pub incident_id: String,
    #[validate(trim, non_empty, len_max = 1500)]
    pub resolution_note: String,
}

#[domain(table = "agentops_workspaces", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentOpsWorkspace {
    name: String,
    agents: PersistJson<Vec<AgentProfile>>,
    missions: PersistJson<Vec<Mission>>,
    runs: PersistJson<Vec<MissionRun>>,
    incidents: PersistJson<Vec<IncidentRecord>>,
}

#[derive(Clone, Debug, PartialEq, Eq, DomainError)]
pub enum AgentOpsError {
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
    #[api_error(status = 404, code = "agent_not_found")]
    AgentNotFound(String),
    #[api_error(status = 404, code = "mission_not_found")]
    MissionNotFound(String),
    #[api_error(status = 404, code = "run_not_found")]
    RunNotFound(String),
    #[api_error(status = 404, code = "incident_not_found")]
    IncidentNotFound(String),
    #[api_error(status = 409, code = "duplicate_agent_handle")]
    DuplicateAgentHandle(String),
    #[api_error(status = 409, code = "agent_inactive")]
    AgentInactive(String),
    #[api_error(status = 409, code = "mission_not_active")]
    MissionNotActive(String),
    #[api_error(status = 409, code = "run_not_running")]
    RunNotRunning(String),
    #[api_error(status = 409, code = "invalid_state_transition")]
    InvalidStateTransition(String),
}

impl std::fmt::Display for AgentOpsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "{message}"),
            Self::AgentNotFound(agent_id) => write!(f, "agent not found: {agent_id}"),
            Self::MissionNotFound(mission_id) => write!(f, "mission not found: {mission_id}"),
            Self::RunNotFound(run_id) => write!(f, "run not found: {run_id}"),
            Self::IncidentNotFound(incident_id) => write!(f, "incident not found: {incident_id}"),
            Self::DuplicateAgentHandle(handle) => write!(f, "agent handle already exists: {handle}"),
            Self::AgentInactive(agent_id) => write!(f, "agent is inactive: {agent_id}"),
            Self::MissionNotActive(mission_id) => write!(f, "mission is not active: {mission_id}"),
            Self::RunNotRunning(run_id) => write!(f, "run is not running: {run_id}"),
            Self::InvalidStateTransition(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for AgentOpsError {}

fn compute_ops_dashboard_view(model: &AgentOpsWorkspace) -> OpsDashboardView {
    let agents_total = model.agents.len();
    let active_agents = model.agents.iter().filter(|agent| agent.active).count();
    let missions_active = model
        .missions
        .iter()
        .filter(|mission| mission.status == MissionStatus::Active)
        .count();
    let runs_running = model
        .runs
        .iter()
        .filter(|run| run.state == RunState::Running || run.state == RunState::WaitingHandoff)
        .count();
    let incidents_open = model
        .incidents
        .iter()
        .filter(|incident| incident.status == IncidentStatus::Open)
        .count();
    let token_cost_total = model.runs.iter().map(|run| run.token_cost_total).sum();
    let slow_steps_over_2s = model
        .runs
        .iter()
        .flat_map(|run| run.steps.iter())
        .filter(|step| step.latency_ms > 2_000)
        .count();

    OpsDashboardView {
        workspace_name: model.name.clone(),
        agents_total,
        active_agents,
        missions_active,
        runs_running,
        incidents_open,
        token_cost_total,
        slow_steps_over_2s,
    }
}

#[api(views(OpsDashboardView, ReliabilityView))]
impl AgentOpsWorkspace {
    pub fn new(name: String) -> Self {
        let normalized = name.trim();
        let resolved_name = if normalized.is_empty() {
            "New AgentOps Workspace".to_string()
        } else {
            truncate_to_len(normalized, WORKSPACE_NAME_MAX_LEN)
        };

        Self {
            name: resolved_name,
            agents: PersistJson::default(),
            missions: PersistJson::default(),
            runs: PersistJson::default(),
            incidents: PersistJson::default(),
        }
    }

    #[command(validate = true)]
    pub fn rename_workspace(
        &mut self,
        input: RenameWorkspaceInput,
    ) -> Result<String, AgentOpsError> {
        self.name = input.name;
        Ok(self.name.clone())
    }

    #[command(validate = true)]
    pub fn register_agent(
        &mut self,
        input: RegisterAgentInput,
    ) -> Result<AgentProfile, AgentOpsError> {
        if self
            .agents
            .iter()
            .any(|agent| agent.handle.eq_ignore_ascii_case(&input.handle))
        {
            return Err(AgentOpsError::DuplicateAgentHandle(input.handle));
        }

        let created = AgentProfile {
            id: Uuid::new_v4().to_string(),
            handle: input.handle,
            model: input.model,
            active: true,
            created_at: Utc::now(),
        };
        self.agents.push(created.clone());
        Ok(created)
    }

    #[command(validate = true)]
    pub fn set_agent_active(
        &mut self,
        input: SetAgentActiveInput,
    ) -> Result<(), AgentOpsError> {
        let agent = find_mut_by_id(self.agents.as_mut_slice(), input.agent_id.as_str(), |agent| {
            agent.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::AgentNotFound(input.agent_id.clone()))?;
        agent.active = input.active;
        Ok(())
    }

    #[command(validate = true)]
    pub fn create_mission(
        &mut self,
        input: CreateMissionInput,
    ) -> Result<Mission, AgentOpsError> {
        let now = Utc::now();
        let mission = Mission {
            id: Uuid::new_v4().to_string(),
            title: input.title,
            objective: input.objective,
            priority: input.priority,
            status: MissionStatus::Draft,
            owner_agent_id: None,
            created_at: now,
            updated_at: now,
        };
        self.missions.push(mission.clone());
        Ok(mission)
    }

    #[command(validate = true)]
    pub fn activate_mission(
        &mut self,
        input: ActivateMissionInput,
    ) -> Result<(), AgentOpsError> {
        ensure_agent_active(&self.agents, &input.owner_agent_id)?;

        let mission = find_mut_by_id(self.missions.as_mut_slice(), input.mission_id.as_str(), |m| {
            m.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::MissionNotFound(input.mission_id.clone()))?;

        if mission.status == MissionStatus::Archived {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "mission {} is archived and cannot be activated",
                input.mission_id
            )));
        }

        mission.status = MissionStatus::Active;
        mission.owner_agent_id = Some(input.owner_agent_id);
        mission.updated_at = Utc::now();
        Ok(())
    }

    #[command(validate = true)]
    pub fn pause_mission(&mut self, input: MissionIdInput) -> Result<(), AgentOpsError> {
        let mission = find_mut_by_id(self.missions.as_mut_slice(), input.mission_id.as_str(), |m| {
            m.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::MissionNotFound(input.mission_id.clone()))?;

        if mission.status != MissionStatus::Active {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "mission {} can be paused only from active status",
                input.mission_id
            )));
        }
        mission.status = MissionStatus::Paused;
        mission.updated_at = Utc::now();
        Ok(())
    }

    #[command(validate = true)]
    pub fn archive_mission(&mut self, input: MissionIdInput) -> Result<(), AgentOpsError> {
        let mission = find_mut_by_id(self.missions.as_mut_slice(), input.mission_id.as_str(), |m| {
            m.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::MissionNotFound(input.mission_id.clone()))?;

        if mission.status == MissionStatus::Archived {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "mission {} is already archived",
                input.mission_id
            )));
        }

        mission.status = MissionStatus::Archived;
        mission.updated_at = Utc::now();
        Ok(())
    }

    #[command(validate = true)]
    pub fn start_run(
        &mut self,
        input: StartRunInput,
    ) -> Result<MissionRun, AgentOpsError> {
        ensure_agent_active(&self.agents, &input.assigned_agent_id)?;
        let mission = find_mut_by_id(self.missions.as_mut_slice(), input.mission_id.as_str(), |m| {
            m.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::MissionNotFound(input.mission_id.clone()))?;
        if mission.status != MissionStatus::Active {
            return Err(AgentOpsError::MissionNotActive(input.mission_id));
        }

        let attempt = self
            .runs
            .iter()
            .filter(|run| run.mission_id == mission.id)
            .map(|run| run.attempt)
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        let run = MissionRun {
            id: Uuid::new_v4().to_string(),
            mission_id: mission.id.clone(),
            assigned_agent_id: input.assigned_agent_id,
            state: RunState::Running,
            attempt,
            steps: Vec::new(),
            token_cost_total: 0,
            started_at: Utc::now(),
            finished_at: None,
            last_error: None,
        };
        mission.updated_at = Utc::now();
        self.runs.push(run.clone());
        Ok(run)
    }

    #[command(validate = true)]
    pub fn append_run_step(
        &mut self,
        input: AppendRunStepInput,
    ) -> Result<RunStep, AgentOpsError> {
        let run = find_mut_by_id(self.runs.as_mut_slice(), input.run_id.as_str(), |run| {
            run.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::RunNotFound(input.run_id.clone()))?;
        if run.state != RunState::Running && run.state != RunState::WaitingHandoff {
            return Err(AgentOpsError::RunNotRunning(input.run_id));
        }

        let step = RunStep {
            id: Uuid::new_v4().to_string(),
            phase: input.phase,
            summary: input.summary,
            latency_ms: input.latency_ms,
            token_cost: input.token_cost,
            created_at: Utc::now(),
        };
        run.token_cost_total = run.token_cost_total.saturating_add(input.token_cost);
        run.steps.push(step.clone());
        if run.state == RunState::WaitingHandoff {
            run.state = RunState::Running;
        }
        Ok(step)
    }

    #[command(validate = true)]
    pub fn handoff_run(
        &mut self,
        input: HandoffRunInput,
    ) -> Result<(), AgentOpsError> {
        ensure_agent_active(&self.agents, &input.to_agent_id)?;

        let run = find_mut_by_id(self.runs.as_mut_slice(), input.run_id.as_str(), |run| {
            run.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::RunNotFound(input.run_id.clone()))?;

        if run.state != RunState::Running {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "run {} can be handed off only from running state",
                input.run_id
            )));
        }

        run.assigned_agent_id = input.to_agent_id;
        run.state = RunState::WaitingHandoff;
        run.steps.push(RunStep {
            id: Uuid::new_v4().to_string(),
            phase: "handoff".to_string(),
            summary: input.note,
            latency_ms: 0,
            token_cost: 0,
            created_at: Utc::now(),
        });
        Ok(())
    }

    #[command(validate = true)]
    pub fn accept_handoff(&mut self, input: RunIdInput) -> Result<(), AgentOpsError> {
        let run = find_mut_by_id(self.runs.as_mut_slice(), input.run_id.as_str(), |run| {
            run.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::RunNotFound(input.run_id.clone()))?;
        if run.state != RunState::WaitingHandoff {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "run {} is not waiting handoff",
                input.run_id
            )));
        }

        run.state = RunState::Running;
        run.steps.push(RunStep {
            id: Uuid::new_v4().to_string(),
            phase: "handoff_ack".to_string(),
            summary: "handoff accepted".to_string(),
            latency_ms: 0,
            token_cost: 0,
            created_at: Utc::now(),
        });
        Ok(())
    }

    #[command(validate = true)]
    pub fn finish_run(&mut self, input: RunIdInput) -> Result<(), AgentOpsError> {
        let run = find_mut_by_id(self.runs.as_mut_slice(), input.run_id.as_str(), |run| {
            run.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::RunNotFound(input.run_id.clone()))?;
        if run.state != RunState::Running && run.state != RunState::WaitingHandoff {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "run {} can be finished only from running or waiting_handoff state",
                input.run_id
            )));
        }
        run.state = RunState::Succeeded;
        run.finished_at = Some(Utc::now());
        run.steps.push(RunStep {
            id: Uuid::new_v4().to_string(),
            phase: "finish".to_string(),
            summary: "run finished successfully".to_string(),
            latency_ms: 0,
            token_cost: 0,
            created_at: Utc::now(),
        });
        Ok(())
    }

    #[command(validate = true)]
    pub fn fail_run(&mut self, input: FailRunInput) -> Result<(), AgentOpsError> {
        let run = find_mut_by_id(self.runs.as_mut_slice(), input.run_id.as_str(), |run| {
            run.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::RunNotFound(input.run_id.clone()))?;
        if run.state != RunState::Running && run.state != RunState::WaitingHandoff {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "run {} can be failed only from running or waiting_handoff state",
                input.run_id
            )));
        }
        run.state = RunState::Failed;
        run.finished_at = Some(Utc::now());
        run.last_error = Some(input.error_message.clone());
        run.steps.push(RunStep {
            id: Uuid::new_v4().to_string(),
            phase: "fail".to_string(),
            summary: input.error_message,
            latency_ms: 0,
            token_cost: 0,
            created_at: Utc::now(),
        });
        Ok(())
    }

    #[command(validate = true)]
    pub fn cancel_run(&mut self, input: CancelRunInput) -> Result<(), AgentOpsError> {
        let run = find_mut_by_id(self.runs.as_mut_slice(), input.run_id.as_str(), |run| {
            run.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::RunNotFound(input.run_id.clone()))?;
        if run.state == RunState::Succeeded
            || run.state == RunState::Failed
            || run.state == RunState::Canceled
        {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "run {} cannot be canceled from current state",
                input.run_id
            )));
        }
        run.state = RunState::Canceled;
        run.finished_at = Some(Utc::now());
        run.last_error = Some(input.reason.clone());
        run.steps.push(RunStep {
            id: Uuid::new_v4().to_string(),
            phase: "cancel".to_string(),
            summary: input.reason,
            latency_ms: 0,
            token_cost: 0,
            created_at: Utc::now(),
        });
        Ok(())
    }

    #[command(validate = true)]
    pub fn retry_run(
        &mut self,
        input: RetryRunInput,
    ) -> Result<MissionRun, AgentOpsError> {
        ensure_agent_active(&self.agents, &input.assigned_agent_id)?;
        let previous = find_by_id(self.runs.as_slice(), input.run_id.as_str(), |run| {
            run.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::RunNotFound(input.run_id.clone()))?;
        if previous.state != RunState::Failed && previous.state != RunState::Canceled {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "run {} can be retried only from failed or canceled state",
                input.run_id
            )));
        }

        let next = MissionRun {
            id: Uuid::new_v4().to_string(),
            mission_id: previous.mission_id.clone(),
            assigned_agent_id: input.assigned_agent_id,
            state: RunState::Running,
            attempt: previous.attempt.saturating_add(1),
            steps: Vec::new(),
            token_cost_total: 0,
            started_at: Utc::now(),
            finished_at: None,
            last_error: None,
        };
        self.runs.push(next.clone());
        Ok(next)
    }

    #[command(validate = true)]
    pub fn raise_incident(
        &mut self,
        input: RaiseIncidentInput,
    ) -> Result<IncidentRecord, AgentOpsError> {
        if let Some(run_id) = input.run_id.as_deref() {
            let run_exists = find_by_id(self.runs.as_slice(), run_id, |run| run.id.as_str()).is_some();
            if !run_exists {
                return Err(AgentOpsError::RunNotFound(run_id.to_string()));
            }
        }

        let incident = IncidentRecord {
            id: Uuid::new_v4().to_string(),
            run_id: input.run_id,
            severity: input.severity,
            status: IncidentStatus::Open,
            title: input.title,
            details: input.details,
            created_at: Utc::now(),
            resolved_at: None,
            resolution_note: None,
        };
        self.incidents.push(incident.clone());
        Ok(incident)
    }

    #[command(validate = true)]
    pub fn resolve_incident(
        &mut self,
        input: ResolveIncidentInput,
    ) -> Result<(), AgentOpsError> {
        let incident = find_mut_by_id(
            self.incidents.as_mut_slice(),
            input.incident_id.as_str(),
            |incident| incident.id.as_str(),
        )
        .ok_or_else(|| AgentOpsError::IncidentNotFound(input.incident_id.clone()))?;

        if incident.status != IncidentStatus::Open && incident.status != IncidentStatus::Mitigated {
            return Err(AgentOpsError::InvalidStateTransition(format!(
                "incident {} is not open/mitigated",
                input.incident_id
            )));
        }

        incident.status = IncidentStatus::Resolved;
        incident.resolved_at = Some(Utc::now());
        incident.resolution_note = Some(input.resolution_note);
        Ok(())
    }

    #[query]
    pub fn run_timeline(&self, run_id: String) -> Result<Vec<RunStep>, AgentOpsError> {
        let run = find_by_id(self.runs.as_slice(), run_id.as_str(), |run| run.id.as_str())
            .ok_or(AgentOpsError::RunNotFound(run_id))?;
        Ok(run.steps.clone())
    }

    #[query]
    pub fn mission_health(&self, mission_id: String) -> Result<MissionHealthSnapshot, AgentOpsError> {
        let mission = find_by_id(self.missions.as_slice(), mission_id.as_str(), |m| m.id.as_str())
            .ok_or_else(|| AgentOpsError::MissionNotFound(mission_id.clone()))?;
        let related_runs = self
            .runs
            .iter()
            .filter(|run| run.mission_id == mission_id)
            .collect::<Vec<_>>();

        let total_runs = related_runs.len();
        let successful_runs = related_runs
            .iter()
            .filter(|run| run.state == RunState::Succeeded)
            .count();
        let failed_runs = related_runs
            .iter()
            .filter(|run| run.state == RunState::Failed)
            .count();

        let mut step_count = 0usize;
        let mut latency_total = 0u64;
        for run in &related_runs {
            for step in &run.steps {
                step_count = step_count.saturating_add(1);
                latency_total = latency_total.saturating_add(step.latency_ms);
            }
        }
        let avg_step_latency_ms = if step_count == 0 {
            None
        } else {
            Some(latency_total as f64 / step_count as f64)
        };

        let mut run_state_breakdown = BTreeMap::<String, usize>::new();
        for run in &related_runs {
            let key = format!("{:?}", run.state);
            *run_state_breakdown.entry(key).or_insert(0) += 1;
        }

        let last_error = related_runs
            .iter()
            .filter_map(|run| {
                run.last_error
                    .as_ref()
                    .map(|error| (run.finished_at.unwrap_or(run.started_at), error.clone()))
            })
            .max_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, error)| error);

        Ok(MissionHealthSnapshot {
            mission_id: mission.id.clone(),
            title: mission.title.clone(),
            status: mission.status,
            total_runs,
            successful_runs,
            failed_runs,
            avg_step_latency_ms,
            last_error,
            run_state_breakdown,
        })
    }

    #[query]
    pub fn agent_load(&self, agent_id: String) -> Result<AgentLoadSnapshot, AgentOpsError> {
        let agent = find_by_id(self.agents.as_slice(), agent_id.as_str(), |agent| {
            agent.id.as_str()
        })
        .ok_or_else(|| AgentOpsError::AgentNotFound(agent_id.clone()))?;

        let running_runs = self
            .runs
            .iter()
            .filter(|run| run.assigned_agent_id == agent.id && run.state == RunState::Running)
            .count();
        let waiting_handoff_runs = self
            .runs
            .iter()
            .filter(|run| {
                run.assigned_agent_id == agent.id && run.state == RunState::WaitingHandoff
            })
            .count();
        let assigned_active_missions = self
            .missions
            .iter()
            .filter(|mission| {
                mission.owner_agent_id.as_deref() == Some(agent.id.as_str())
                    && mission.status == MissionStatus::Active
            })
            .count();

        Ok(AgentLoadSnapshot {
            agent_id: agent.id.clone(),
            handle: agent.handle.clone(),
            active: agent.active,
            running_runs,
            waiting_handoff_runs,
            assigned_active_missions,
        })
    }

    #[query]
    pub fn open_incidents(&self) -> Vec<IncidentRecord> {
        self.incidents
            .iter()
            .filter(|incident| incident.status == IncidentStatus::Open)
            .cloned()
            .collect()
    }
}

fn ensure_agent_active(
    agents: &[AgentProfile],
    agent_id: &str,
) -> Result<(), AgentOpsError> {
    let agent = find_by_id(agents, agent_id, |agent| agent.id.as_str())
        .ok_or_else(|| AgentOpsError::AgentNotFound(agent_id.to_string()))?;
    if !agent.active {
        return Err(AgentOpsError::AgentInactive(agent_id.to_string()));
    }
    Ok(())
}

#[allow(clippy::manual_find)]
fn find_by_id<'a, T, F>(items: &'a [T], id: &str, mut id_of: F) -> Option<&'a T>
where
    F: FnMut(&T) -> &str,
{
    for item in items {
        if id_of(item) == id {
            return Some(item);
        }
    }
    None
}

#[allow(clippy::manual_find)]
fn find_mut_by_id<'a, T, F>(items: &'a mut [T], id: &str, mut id_of: F) -> Option<&'a mut T>
where
    F: FnMut(&T) -> &str,
{
    for item in items {
        if id_of(item) == id {
            return Some(item);
        }
    }
    None
}

fn truncate_to_len(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        value.to_string()
    } else {
        value.chars().take(max_len).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ActivateMissionInput, AgentOpsWorkspace, AppendRunStepInput, CreateMissionInput,
        FailRunInput, HandoffRunInput, IncidentSeverity, MissionIdInput, RaiseIncidentInput,
        RegisterAgentInput, ResolveIncidentInput, RetryRunInput, RunIdInput, RunState,
        StartRunInput,
    };

    #[test]
    fn workspace_domain_flow_handles_handoff_retry_and_incident() {
        let mut workspace = AgentOpsWorkspace::new("Mission Control".to_string());
        let agent_a = workspace
            .register_agent(RegisterAgentInput {
                handle: "@alpha".to_string(),
                model: "gpt-5".to_string(),
            })
            .expect("register agent a");
        let agent_b = workspace
            .register_agent(RegisterAgentInput {
                handle: "@beta".to_string(),
                model: "gpt-5".to_string(),
            })
            .expect("register agent b");
        let mission = workspace
            .create_mission(CreateMissionInput {
                title: "Incident Triage".to_string(),
                objective: "Classify and route on-call pages".to_string(),
                priority: 5,
            })
            .expect("create mission");
        workspace
            .activate_mission(ActivateMissionInput {
                mission_id: mission.id.clone(),
                owner_agent_id: agent_a.id.clone(),
            })
            .expect("activate mission");
        let run = workspace
            .start_run(StartRunInput {
                mission_id: mission.id.clone(),
                assigned_agent_id: agent_a.id.clone(),
            })
            .expect("start run");
        workspace
            .append_run_step(AppendRunStepInput {
                run_id: run.id.clone(),
                phase: "classify".to_string(),
                summary: "classified incident as database saturation".to_string(),
                latency_ms: 420,
                token_cost: 180,
            })
            .expect("append step");
        workspace
            .handoff_run(HandoffRunInput {
                run_id: run.id.clone(),
                to_agent_id: agent_b.id.clone(),
                note: "need db specialist".to_string(),
            })
            .expect("handoff");
        workspace
            .accept_handoff(RunIdInput {
                run_id: run.id.clone(),
            })
            .expect("accept handoff");
        workspace
            .fail_run(FailRunInput {
                run_id: run.id.clone(),
                error_message: "insufficient context".to_string(),
            })
            .expect("fail run");
        let retry = workspace
            .retry_run(RetryRunInput {
                run_id: run.id.clone(),
                assigned_agent_id: agent_b.id.clone(),
            })
            .expect("retry run");
        workspace
            .finish_run(RunIdInput {
                run_id: retry.id.clone(),
            })
            .expect("finish retry run");
        let incident = workspace
            .raise_incident(RaiseIncidentInput {
                run_id: Some(retry.id.clone()),
                severity: IncidentSeverity::High,
                title: "Data lag".to_string(),
                details: "observed lag in replicated shard".to_string(),
            })
            .expect("raise incident");
        workspace
            .resolve_incident(ResolveIncidentInput {
                incident_id: incident.id.clone(),
                resolution_note: "replica restarted".to_string(),
            })
            .expect("resolve incident");

        let timeline = workspace
            .run_timeline(run.id.clone())
            .expect("run timeline");
        assert!(!timeline.is_empty());
        let health = workspace
            .mission_health(mission.id.clone())
            .expect("mission health");
        assert_eq!(health.failed_runs, 1);
        assert_eq!(health.successful_runs, 1);
        let open_incidents = workspace.open_incidents();
        assert!(open_incidents.is_empty());
        let mut retry_run = None;
        for entry in workspace.runs.iter() {
            if entry.id == retry.id {
                retry_run = Some(entry);
                break;
            }
        }
        let retry_run = retry_run.expect("retry run persisted");
        assert_eq!(retry_run.state, RunState::Succeeded);
    }

    #[test]
    fn duplicate_agent_handle_is_rejected() {
        let mut workspace = AgentOpsWorkspace::new("Mission Control".to_string());
        workspace
            .register_agent(RegisterAgentInput {
                handle: "@alpha".to_string(),
                model: "gpt-5".to_string(),
            })
            .expect("register first");
        let error = workspace
            .register_agent(RegisterAgentInput {
                handle: "@ALPHA".to_string(),
                model: "gpt-5-mini".to_string(),
            })
            .expect_err("duplicate must fail");
        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn mission_pause_and_archive_accepts_single_field_dto() {
        let mut workspace = AgentOpsWorkspace::new("Mission Control".to_string());
        let owner = workspace
            .register_agent(RegisterAgentInput {
                handle: "@alpha".to_string(),
                model: "gpt-5".to_string(),
            })
            .expect("register owner");
        let mission = workspace
            .create_mission(CreateMissionInput {
                title: "Quality".to_string(),
                objective: "check lifecycle dto commands".to_string(),
                priority: 3,
            })
            .expect("create mission");
        workspace
            .activate_mission(ActivateMissionInput {
                mission_id: mission.id.clone(),
                owner_agent_id: owner.id.clone(),
            })
            .expect("activate mission");

        workspace
            .pause_mission(MissionIdInput {
                mission_id: mission.id.clone(),
            })
            .expect("pause mission");
        workspace
            .archive_mission(MissionIdInput {
                mission_id: mission.id.clone(),
            })
            .expect("archive mission");
    }
}
