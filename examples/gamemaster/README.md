# 🎮 GameMaster: High-Performance Matchmaking Server

A production-ready example of a multiplayer game backend built with `RustMemDB`.

**The goal:** Show how to build a modern game backend without repository/ORM boilerplate and without infrastructure code in app logic.

## 🚀 Key Features

1.  **In-Memory Matchmaking:** Finding opponents is instant (`O(N)` scan in RAM is faster than SQL network roundtrip).
2.  **Atomic Workflow:** `finish_match` updates lobby state and appends match history in one workflow call.
3.  **Crash Recovery:** Kill the server, restart it -> Leaderboard and Stats are restored perfectly.
4.  **Zero DB Config:** No `docker-compose`, no migrations to run. Just `cargo run`.

## 🆚 Comparison

| Feature | Standard Way (SQL + Redis) | RustMemDB Way |
| :--- | :--- | :--- |
| **Setup** | Install Postgres, Redis, Run Migrations | Add crate to `Cargo.toml` |
| **Matchmaking** | Complex SQL query or Redis sorted sets | Native Rust `Vec` iteration/filtering |
| **Transactions** | `BEGIN; UPDATE...; INSERT...; COMMIT;` | `execute_workflow_if_match_with_create(...)` |
| **Latency** | Network I/O for every read/write | Microsecond RAM access |
| **Code Size** | Heavy (Entities + ORM mappings + Repos) | Light (Just Domain Structs) |

## 🛠️ How to Run

```bash
# Start the server
cargo run --manifest-path examples/gamemaster/Cargo.toml
```

### Test API

**1. Register Players**
```bash
curl -X POST -H "Content-Type: application/json" -d '{"username": "Hero"}' http://localhost:3000/players
curl -X POST -H "Content-Type: application/json" -d '{"username": "Villain"}' http://localhost:3000/players
```

**2. Queue for Match** (Villain looks for Hero)
```bash
# Returns "waiting"
curl -X POST http://localhost:3000/matchmaking/Villain_ID

# Returns "match_found" with Lobby ID
curl -X POST http://localhost:3000/matchmaking/Hero_ID
```

**3. Finish Match** (Hero wins)
```bash
curl -X POST -H "Content-Type: application/json" -d '{"winner_id": "Hero_ID"}' http://localhost:3000/lobbies/LOBBY_ID/finish
```

**4. Check Leaderboard**
```bash
curl http://localhost:3000/leaderboard
```

## 🧠 Code Highlights

Check `src/service.rs`:

```rust
// One intent-style call instead of manual transaction orchestration.
self.lobbies
    .workflow_with_create(
        &mut self.history,
        lobby_id,
        workflow,
    )
    .await?;
```

`GameService` also uses quick-start domain API where workflow orchestration is not needed:

```rust
let players = app.open_domain::<PlayerVec>("players").await?;
```
