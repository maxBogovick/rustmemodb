use crate::domain::{FinishMatchWorkflow, Lobby, Player};
use rustmemodb::{DbError, PersistApp, PersistDomainStore, Result};
use std::collections::HashMap;

// Use the type aliases generated in main.rs
use crate::{HistoryVec, LobbyVec, PlayerVec};

pub struct GameService {
    players: PersistDomainStore<PlayerVec>,
    lobbies: PersistDomainStore<LobbyVec>,
    history: PersistDomainStore<HistoryVec>,
}

impl GameService {
    pub async fn new(app: &PersistApp) -> Result<Self> {
        Ok(Self {
            players: app.open_domain("players").await?,
            lobbies: app.open_domain("lobbies").await?,
            history: app.open_domain("history").await?,
        })
    }

    pub async fn register(&mut self, username: String) -> Result<String> {
        let player = Player::create(username);
        let id = player.persist_id().to_string();
        self.players.create(player).await?;
        Ok(id)
    }

    pub async fn queue_match(&mut self, player_id: &str) -> Result<Option<String>> {
        let me = self
            .players
            .get(player_id)
            .ok_or(DbError::ExecutionError("Player not found".into()))?;

        if self.player_in_active_lobby(player_id) {
            return Ok(None);
        }

        let my_username = me.username().clone();
        let opponent_id = self.players.list().iter().find_map(|candidate| {
            if candidate.persist_id() == player_id {
                return None;
            }
            if candidate.username() == &my_username {
                return None;
            }
            if self.player_in_active_lobby(candidate.persist_id()) {
                return None;
            }
            Some(candidate.persist_id().to_string())
        });

        if let Some(opponent_id) = opponent_id {
            let lobby = Lobby::create(player_id.to_string(), opponent_id);
            let lobby_id = lobby.persist_id().to_string();
            self.lobbies.create(lobby).await?;
            Ok(Some(lobby_id))
        } else {
            Ok(None)
        }
    }

    pub async fn finish_match(&mut self, lobby_id: &str, winner_id: &str) -> Result<()> {
        let lobby = self
            .lobbies
            .get(lobby_id)
            .cloned()
            .ok_or(DbError::ExecutionError("Lobby not found".into()))?;

        if !lobby.is_in_progress() {
            return Err(DbError::ExecutionError(
                "Lobby is already finished".to_string(),
            ));
        }

        if !lobby.has_player(winner_id) {
            return Err(DbError::ExecutionError(
                "Winner is not a participant of this lobby".to_string(),
            ));
        }

        let loser_id = if lobby.player1_id() == winner_id {
            lobby.player2_id().clone()
        } else {
            lobby.player1_id().clone()
        };

        let workflow = FinishMatchWorkflow::new(
            winner_id.to_string(),
            loser_id,
            25,
            chrono::Utc::now().timestamp(),
        );

        let updated = self
            .lobbies
            .workflow_with_create(&mut self.history, lobby_id, workflow)
            .await?;
        if updated.is_none() {
            return Err(DbError::ExecutionError(
                "Lobby changed during finish_match".to_string(),
            ));
        }

        Ok(())
    }

    pub fn leaderboard(&self) -> Vec<(String, i32)> {
        let mut usernames_by_id = HashMap::<String, String>::new();
        let mut rating_by_id = HashMap::<String, i32>::new();

        for player in self.players.list() {
            let id = player.persist_id().to_string();
            usernames_by_id.insert(id.clone(), player.username().clone());
            rating_by_id.insert(id, 1000);
        }

        for result in self.history.list() {
            let winner = result.winner_id().clone();
            let loser = result.loser_id().clone();
            let delta = *result.delta_mmr();
            *rating_by_id.entry(winner).or_insert(1000) += delta;
            *rating_by_id.entry(loser).or_insert(1000) -= delta;
        }

        let mut board = usernames_by_id
            .into_iter()
            .map(|(id, username)| {
                let rating = *rating_by_id.get(&id).unwrap_or(&1000);
                (username, rating)
            })
            .collect::<Vec<_>>();
        board.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        board.truncate(10);
        board
    }

    fn player_in_active_lobby(&self, player_id: &str) -> bool {
        self.lobbies
            .list()
            .iter()
            .any(|lobby| lobby.is_in_progress() && lobby.has_player(player_id))
    }
}
