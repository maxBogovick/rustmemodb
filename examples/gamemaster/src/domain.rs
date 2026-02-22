use rustmemodb::{PersistJsonValue, PersistWorkflowCommandModel, persist_struct};
use serde::{Deserialize, Serialize};

// --- Player Entity ---
persist_struct! {
    pub struct Player table = "players" {
        #[persist(unique)]
        username: String,
    }
}

impl Player {
    pub fn create(username: String) -> Self {
        Self::new(username)
    }
}

// --- Lobby Entity ---
persist_struct! {
    pub struct Lobby table = "lobbies" {
        player1_id: String,
        player2_id: String,
        state: LobbyState,
        created_at: i64,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PersistJsonValue)]
pub enum LobbyState {
    Waiting,
    InProgress,
    Finished { winner_id: Option<String> },
}

impl Lobby {
    pub fn create(p1: String, p2: String) -> Self {
        Self::new(p1, p2, LobbyState::InProgress, chrono::Utc::now().timestamp_millis())
    }

    pub fn is_in_progress(&self) -> bool {
        matches!(self.state(), LobbyState::InProgress)
    }

    pub fn has_player(&self, player_id: &str) -> bool {
        self.player1_id() == player_id || self.player2_id() == player_id
    }
}

// --- Match Result Entity ---
persist_struct! {
    pub struct MatchResult table = "match_history" {
        #[persist(index)]
        lobby_id: String,
        #[persist(index)]
        winner_id: String,
        #[persist(index)]
        loser_id: String,
        delta_mmr: i32,
        finished_at: i64,
    }
}

#[derive(Debug, Clone)]
pub struct FinishMatchWorkflow {
    pub winner_id: String,
    pub loser_id: String,
    pub delta_mmr: i32,
    pub finished_at: i64,
}

impl FinishMatchWorkflow {
    pub fn new(winner_id: String, loser_id: String, delta_mmr: i32, finished_at: i64) -> Self {
        Self {
            winner_id,
            loser_id,
            delta_mmr,
            finished_at,
        }
    }
}

// Single business action -> atomic lobby update + history append.
impl PersistWorkflowCommandModel<FinishMatchWorkflow, MatchResult> for Lobby {
    fn to_persist_command(command: &FinishMatchWorkflow) -> LobbyCommand {
        LobbyCommand::SetState(LobbyState::Finished {
            winner_id: Some(command.winner_id.clone()),
        })
    }

    fn to_related_record(
        command: &FinishMatchWorkflow,
        updated: &Self,
    ) -> rustmemodb::Result<MatchResult> {
        Ok(MatchResult::new(
            updated.persist_id().to_string(),
            command.winner_id.clone(),
            command.loser_id.clone(),
            command.delta_mmr,
            command.finished_at,
        ))
    }
}
