use pulse_studio::model::{PulseStudioError, PulseWorkspace};

#[test]
fn workspace_domain_flow_updates_dashboard() {
    let mut workspace = PulseWorkspace::new("Pulse Team".to_string());
    let channel = workspace
        .add_channel("YouTube".to_string(), "PulseTeam".to_string(), true)
        .expect("add channel");
    let campaign = workspace
        .launch_campaign(channel.id.clone(), "Launch Week".to_string(), 50000)
        .expect("launch campaign");

    workspace
        .record_spend(campaign.id, 12000)
        .expect("record spend");

    let dashboard = workspace.workspace_dashboard();
    assert_eq!(dashboard.channels_total, 1);
    assert_eq!(dashboard.campaigns_total, 1);
    assert_eq!(dashboard.spent_total_minor, 12000);
}

#[test]
fn record_spend_rejects_budget_overflow() {
    let mut workspace = PulseWorkspace::new("Pulse Team".to_string());
    let channel = workspace
        .add_channel("TikTok".to_string(), "@pulse".to_string(), true)
        .expect("add channel");
    let campaign = workspace
        .launch_campaign(channel.id, "Flash Sale".to_string(), 1000)
        .expect("launch campaign");

    let error = workspace
        .record_spend(campaign.id, 1500)
        .expect_err("spend above budget must fail");

    match error {
        PulseStudioError::BudgetExceeded { .. } => {}
        other => panic!("expected budget exceeded, got {other:?}"),
    }
}

#[test]
fn record_spend_rejects_paused_campaign() {
    let mut workspace = PulseWorkspace::new("Pulse Team".to_string());
    let channel = workspace
        .add_channel("Instagram".to_string(), "@pulse_ig".to_string(), true)
        .expect("add channel");
    let campaign = workspace
        .launch_campaign(channel.id.clone(), "Paused Campaign".to_string(), 10000)
        .expect("launch campaign");

    workspace
        .set_channel_active(channel.id, false)
        .expect("pause channel");

    let error = workspace
        .record_spend(campaign.id, 1000)
        .expect_err("spend on paused campaign must fail");
    match error {
        PulseStudioError::CampaignNotRunning { .. } => {}
        other => panic!("expected campaign_not_running, got {other:?}"),
    }
}

#[test]
fn launch_campaign_rejects_inactive_channel() {
    let mut workspace = PulseWorkspace::new("Pulse Team".to_string());
    let channel = workspace
        .add_channel("YouTube".to_string(), "@inactive_channel".to_string(), false)
        .expect("add channel");

    let error = workspace
        .launch_campaign(channel.id, "Inactive Campaign".to_string(), 1000)
        .expect_err("launch on inactive channel must fail");
    match error {
        PulseStudioError::ChannelInactive(_) => {}
        other => panic!("expected channel_inactive, got {other:?}"),
    }
}
