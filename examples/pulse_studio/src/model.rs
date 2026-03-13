use chrono::{DateTime, Utc};
use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;

const WORKSPACE_NAME_MAX_LEN: usize = 64;
const PLATFORM_MAX_LEN: usize = 32;
const HANDLE_MAX_LEN: usize = 64;
const CAMPAIGN_TITLE_MAX_LEN: usize = 120;
const EVENT_TYPE_MAX_LEN: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct PulseChannel {
    pub id: String,
    pub platform: String,
    pub handle: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct PulseCampaign {
    pub id: String,
    pub channel_id: String,
    pub title: String,
    pub budget_minor: i64,
    pub spent_minor: i64,
    pub status: PulseCampaignStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
#[serde(rename_all = "snake_case")]
pub enum PulseCampaignStatus {
    Running,
    Paused,
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistJsonValue)]
pub struct PulseActivityEvent {
    pub id: String,
    pub campaign_id: String,
    pub event_type: String,
    pub amount_minor: Option<i64>,
    pub points: Option<i64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistView)]
#[persist_view(
    model = PulseWorkspace,
    name = "dashboard",
    compute = compute_workspace_dashboard_view
)]
pub struct PulseDashboard {
    pub workspace_name: String,
    pub channels_total: usize,
    pub active_channels: usize,
    pub campaigns_total: usize,
    pub running_campaigns: usize,
    pub budget_total_minor: i64,
    pub spent_total_minor: i64,
    pub engagement_points_total: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PersistView)]
#[persist_view(model = PulseWorkspace, name = "insights")]
pub struct PulseInsightsView {
    #[view_metric(kind = "copy", source = "name")]
    pub workspace_name: String,
    #[view_metric(kind = "count", source = "channels")]
    pub channels_total: i64,
    #[view_metric(kind = "count", source = "campaigns")]
    pub campaigns_total: i64,
    #[view_metric(kind = "sum", source = "campaigns", field = "budget_minor")]
    pub budget_total_minor: i64,
    #[view_metric(kind = "sum", source = "campaigns", field = "spent_minor")]
    pub spent_total_minor: i64,
    #[view_metric(kind = "group_by", source = "campaigns", by = "status", op = "count")]
    pub campaigns_by_status: BTreeMap<String, i64>,
    #[view_metric(kind = "group_by", source = "campaigns", by = "status", field = "spent_minor")]
    pub spent_by_status: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PulseCampaignProgress {
    pub campaign_id: String,
    pub title: String,
    pub status: PulseCampaignStatus,
    pub budget_minor: i64,
    pub spent_minor: i64,
    pub remaining_minor: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PulseChannelOverview {
    pub channel_id: String,
    pub platform: String,
    pub handle: String,
    pub active: bool,
    pub campaigns_total: usize,
    pub running_campaigns: usize,
    pub budget_total_minor: i64,
    pub spent_total_minor: i64,
    pub engagement_points_total: i64,
}

#[domain(table = "pulse_workspaces", schema_version = 3)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PulseWorkspace {
    name: String,
    channels: PersistJson<Vec<PulseChannel>>,
    campaigns: PersistJson<Vec<PulseCampaign>>,
    activity: PersistJson<Vec<PulseActivityEvent>>,
}

#[derive(Clone, Debug, PartialEq, Eq, DomainError)]
pub enum PulseStudioError {
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
    #[api_error(status = 404, code = "channel_not_found")]
    ChannelNotFound(String),
    #[api_error(status = 404, code = "campaign_not_found")]
    CampaignNotFound(String),
    #[api_error(status = 409, code = "duplicate_channel_handle")]
    DuplicateChannelHandle(String),
    #[api_error(status = 409, code = "channel_inactive")]
    ChannelInactive(String),
    #[api_error(status = 409, code = "budget_exceeded")]
    BudgetExceeded {
        campaign_id: String,
        budget_minor: i64,
        attempted_spent_minor: i64,
    },
    #[api_error(status = 409, code = "campaign_not_running")]
    CampaignNotRunning {
        campaign_id: String,
        status: PulseCampaignStatus,
    },
}

impl std::fmt::Display for PulseStudioError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "{message}"),
            Self::ChannelNotFound(channel_id) => write!(f, "channel not found: {channel_id}"),
            Self::CampaignNotFound(campaign_id) => {
                write!(f, "campaign not found: {campaign_id}")
            }
            Self::DuplicateChannelHandle(handle) => {
                write!(f, "channel handle already exists: {handle}")
            }
            Self::ChannelInactive(channel_id) => {
                write!(f, "channel is inactive: {channel_id}")
            }
            Self::BudgetExceeded {
                campaign_id,
                budget_minor,
                attempted_spent_minor,
            } => write!(
                f,
                "budget exceeded for {campaign_id}: budget={budget_minor}, attempted={attempted_spent_minor}"
            ),
            Self::CampaignNotRunning {
                campaign_id,
                status,
            } => write!(
                f,
                "campaign {campaign_id} is not running, current status: {:?}",
                status
            ),
        }
    }
}

impl std::error::Error for PulseStudioError {}

fn compute_workspace_dashboard_view(model: &PulseWorkspace) -> PulseDashboard {
    model.workspace_dashboard()
}

#[api(views(PulseDashboard, PulseInsightsView))]
impl PulseWorkspace {
    pub fn new(name: String) -> Self {
        let normalized = name.trim();
        let resolved_name = if normalized.is_empty() {
            "New Pulse Workspace".to_string()
        } else {
            clamp_to_max_len(normalized, WORKSPACE_NAME_MAX_LEN)
        };

        Self {
            name: resolved_name,
            channels: PersistJson::default(),
            campaigns: PersistJson::default(),
            activity: PersistJson::default(),
        }
    }

    #[command]
    pub fn rename_workspace(&mut self, name: String) -> Result<String, PulseStudioError> {
        self.name = normalize_required_bounded(name, "name", WORKSPACE_NAME_MAX_LEN)?;
        Ok(self.name.clone())
    }

    #[command]
    pub fn add_channel(
        &mut self,
        platform: String,
        handle: String,
        active: bool,
    ) -> Result<PulseChannel, PulseStudioError> {
        let platform = normalize_required_bounded(platform, "platform", PLATFORM_MAX_LEN)?;
        let handle = normalize_handle(handle)?;

        if self
            .channels
            .iter()
            .any(|channel| {
                channel.handle.eq_ignore_ascii_case(&handle)
                    && channel.platform.eq_ignore_ascii_case(&platform)
            })
        {
            return Err(PulseStudioError::DuplicateChannelHandle(handle));
        }

        let channel = PulseChannel {
            id: Uuid::new_v4().to_string(),
            platform,
            handle,
            active,
            created_at: Utc::now(),
        };
        self.channels.push(channel.clone());
        Ok(channel)
    }

    #[command]
    pub fn set_channel_active(
        &mut self,
        channel_id: String,
        active: bool,
    ) -> Result<(), PulseStudioError> {
        let idx = self
            .channels
            .iter()
            .position(|channel| channel.id == channel_id)
            .ok_or_else(|| PulseStudioError::ChannelNotFound(channel_id.clone()))?;

        self.channels[idx].active = active;

        for campaign in self
            .campaigns
            .iter_mut()
            .filter(|campaign| campaign.channel_id == channel_id)
        {
            if !active && campaign.status == PulseCampaignStatus::Running {
                campaign.status = PulseCampaignStatus::Paused;
            }
            if active
                && campaign.status == PulseCampaignStatus::Paused
                && campaign.spent_minor < campaign.budget_minor
            {
                campaign.status = PulseCampaignStatus::Running;
            }
        }

        Ok(())
    }

    #[command]
    pub fn launch_campaign(
        &mut self,
        channel_id: String,
        title: String,
        budget_minor: i64,
    ) -> Result<PulseCampaign, PulseStudioError> {
        let title = normalize_required_bounded(title, "title", CAMPAIGN_TITLE_MAX_LEN)?;
        if budget_minor <= 0 {
            return Err(PulseStudioError::Validation(
                "budget_minor must be greater than zero".to_string(),
            ));
        }

        let channel = self
            .channels
            .iter()
            .find(|channel| channel.id == channel_id)
            .ok_or_else(|| PulseStudioError::ChannelNotFound(channel_id.clone()))?;
        if !channel.active {
            return Err(PulseStudioError::ChannelInactive(channel_id));
        }

        let campaign = PulseCampaign {
            id: Uuid::new_v4().to_string(),
            channel_id,
            title,
            budget_minor,
            spent_minor: 0,
            status: PulseCampaignStatus::Running,
            created_at: Utc::now(),
        };

        self.campaigns.push(campaign.clone());
        Ok(campaign)
    }

    #[command]
    pub fn record_spend(
        &mut self,
        campaign_id: String,
        amount_minor: i64,
    ) -> Result<PulseCampaignProgress, PulseStudioError> {
        if amount_minor <= 0 {
            return Err(PulseStudioError::Validation(
                "amount_minor must be greater than zero".to_string(),
            ));
        }

        let idx = self
            .campaigns
            .iter()
            .position(|campaign| campaign.id == campaign_id)
            .ok_or_else(|| PulseStudioError::CampaignNotFound(campaign_id.clone()))?;

        let (campaign_id, title, status, budget_minor, spent_minor) = {
            let campaign = &mut self.campaigns[idx];
            if campaign.status != PulseCampaignStatus::Running {
                return Err(PulseStudioError::CampaignNotRunning {
                    campaign_id: campaign.id.clone(),
                    status: campaign.status,
                });
            }
            let channel_active = self
                .channels
                .iter()
                .find(|channel| channel.id == campaign.channel_id)
                .map(|channel| channel.active)
                .ok_or_else(|| PulseStudioError::ChannelNotFound(campaign.channel_id.clone()))?;
            if !channel_active {
                return Err(PulseStudioError::ChannelInactive(campaign.channel_id.clone()));
            }
            let attempted_spent_minor = campaign.spent_minor.saturating_add(amount_minor);
            if attempted_spent_minor > campaign.budget_minor {
                return Err(PulseStudioError::BudgetExceeded {
                    campaign_id: campaign.id.clone(),
                    budget_minor: campaign.budget_minor,
                    attempted_spent_minor,
                });
            }

            campaign.spent_minor = attempted_spent_minor;
            if campaign.spent_minor == campaign.budget_minor {
                campaign.status = PulseCampaignStatus::Completed;
            }

            (
                campaign.id.clone(),
                campaign.title.clone(),
                campaign.status,
                campaign.budget_minor,
                campaign.spent_minor,
            )
        };

        self.activity.push(PulseActivityEvent {
            id: Uuid::new_v4().to_string(),
            campaign_id: campaign_id.clone(),
            event_type: "spend".to_string(),
            amount_minor: Some(amount_minor),
            points: None,
            created_at: Utc::now(),
        });

        Ok(PulseCampaignProgress {
            campaign_id,
            title,
            status,
            budget_minor,
            spent_minor,
            remaining_minor: budget_minor.saturating_sub(spent_minor),
        })
    }

    #[command]
    pub fn record_engagement(
        &mut self,
        campaign_id: String,
        event_type: String,
        points: i64,
    ) -> Result<i64, PulseStudioError> {
        if points <= 0 {
            return Err(PulseStudioError::Validation(
                "points must be greater than zero".to_string(),
            ));
        }

        if !self
            .campaigns
            .iter()
            .any(|campaign| campaign.id == campaign_id)
        {
            return Err(PulseStudioError::CampaignNotFound(campaign_id));
        }

        let normalized_event_type = normalize_event_type(event_type)?;

        self.activity.push(PulseActivityEvent {
            id: Uuid::new_v4().to_string(),
            campaign_id: campaign_id.clone(),
            event_type: normalized_event_type,
            amount_minor: None,
            points: Some(points),
            created_at: Utc::now(),
        });

        let total = self
            .activity
            .iter()
            .filter(|entry| entry.campaign_id == campaign_id)
            .filter_map(|entry| entry.points)
            .sum();
        Ok(total)
    }

    #[query]
    pub fn workspace_dashboard(&self) -> PulseDashboard {
        let channels_total = self.channels.len();
        let active_channels = self.channels.iter().filter(|channel| channel.active).count();
        let mut campaigns_total = 0usize;
        let mut running_campaigns = 0usize;
        let mut budget_total_minor = 0i64;
        let mut spent_total_minor = 0i64;
        for campaign in self.campaigns.iter() {
            campaigns_total += 1;
            if campaign.status == PulseCampaignStatus::Running {
                running_campaigns += 1;
            }
            budget_total_minor += campaign.budget_minor;
            spent_total_minor += campaign.spent_minor;
        }
        let engagement_points_total = self.activity.iter().filter_map(|event| event.points).sum();

        PulseDashboard {
            workspace_name: self.name.clone(),
            channels_total,
            active_channels,
            campaigns_total,
            running_campaigns,
            budget_total_minor,
            spent_total_minor,
            engagement_points_total,
        }
    }

    #[query]
    pub fn campaign_progress(
        &self,
        campaign_id: String,
    ) -> Result<PulseCampaignProgress, PulseStudioError> {
        let campaign = self
            .campaigns
            .iter()
            .find(|campaign| campaign.id == campaign_id)
            .ok_or(PulseStudioError::CampaignNotFound(campaign_id))?;

        Ok(PulseCampaignProgress {
            campaign_id: campaign.id.clone(),
            title: campaign.title.clone(),
            status: campaign.status,
            budget_minor: campaign.budget_minor,
            spent_minor: campaign.spent_minor,
            remaining_minor: campaign.budget_minor.saturating_sub(campaign.spent_minor),
        })
    }

    #[query]
    pub fn channel_overview(
        &self,
        channel_id: String,
    ) -> Result<PulseChannelOverview, PulseStudioError> {
        let channel = self
            .channels
            .iter()
            .find(|channel| channel.id == channel_id)
            .ok_or(PulseStudioError::ChannelNotFound(channel_id))?;
        let mut campaigns_total = 0usize;
        let mut running_campaigns = 0usize;
        let mut budget_total_minor = 0i64;
        let mut spent_total_minor = 0i64;
        let mut campaign_ids = HashSet::<&str>::new();
        for campaign in self
            .campaigns
            .iter()
            .filter(|campaign| campaign.channel_id == channel.id)
        {
            campaigns_total += 1;
            if campaign.status == PulseCampaignStatus::Running {
                running_campaigns += 1;
            }
            budget_total_minor += campaign.budget_minor;
            spent_total_minor += campaign.spent_minor;
            campaign_ids.insert(campaign.id.as_str());
        }

        let mut engagement_points_total = 0i64;
        for event in self.activity.iter() {
            if campaign_ids.contains(event.campaign_id.as_str()) {
                engagement_points_total += event.points.unwrap_or(0);
            }
        }

        Ok(PulseChannelOverview {
            channel_id: channel.id.clone(),
            platform: channel.platform.clone(),
            handle: channel.handle.clone(),
            active: channel.active,
            campaigns_total,
            running_campaigns,
            budget_total_minor,
            spent_total_minor,
            engagement_points_total,
        })
    }
}

fn normalize_required_bounded(
    value: String,
    field_name: &str,
    max_len: usize,
) -> Result<String, PulseStudioError> {
    let normalized = value.trim().to_string();
    if normalized.is_empty() {
        return Err(PulseStudioError::Validation(format!(
            "{field_name} must not be empty"
        )));
    }
    if normalized.chars().count() > max_len {
        return Err(PulseStudioError::Validation(format!(
            "{field_name} must be at most {max_len} chars"
        )));
    }
    Ok(normalized)
}

fn normalize_handle(value: String) -> Result<String, PulseStudioError> {
    let mut normalized = normalize_required_bounded(value, "handle", HANDLE_MAX_LEN)?
        .to_lowercase()
        .replace(' ', "");
    if !normalized.starts_with('@') {
        normalized = format!("@{normalized}");
    }
    if normalized.chars().count() > HANDLE_MAX_LEN {
        return Err(PulseStudioError::Validation(format!(
            "handle must be at most {HANDLE_MAX_LEN} chars"
        )));
    }
    if !normalized
        .chars()
        .skip(1)
        .all(|ch| {
            ch.is_ascii_lowercase()
                || ch.is_ascii_digit()
                || ch == '_'
                || ch == '.'
                || ch == '-'
        })
    {
        return Err(PulseStudioError::Validation(
            "handle contains unsupported characters".to_string(),
        ));
    }
    Ok(normalized)
}

fn normalize_event_type(value: String) -> Result<String, PulseStudioError> {
    let normalized =
        normalize_required_bounded(value, "event_type", EVENT_TYPE_MAX_LEN)?.to_lowercase();
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
    {
        return Err(PulseStudioError::Validation(
            "event_type contains unsupported characters".to_string(),
        ));
    }
    Ok(normalized)
}

fn clamp_to_max_len(value: &str, max_len: usize) -> String {
    value.chars().take(max_len).collect()
}
