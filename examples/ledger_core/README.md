# LedgerCore Example

LedgerCore is a no-DB-style personal finance system built on `persist` high-level APIs.

What it demonstrates:

- Double-entry bookkeeping (opening balances + transfers).
- Atomic money movements with rollback on business errors.
- Multi-currency transfers (explicit target amount for FX).
- Instant balance reports from in-memory state.
- Domain event stream (`transactions`) persisted as part of aggregate state.
- Persist audit stream available from high-level autonomous commands.
- Nested collections persisted through `PersistJson<Vec<...>>` (no local wrapper types).
- Stable high-level imports through `rustmemodb::prelude::dx::*`.
- Command idempotency enabled by default via `Idempotency-Key`.

HTTP endpoints (generated automatically via `#[expose_rest]` + `#[command]`/`#[query]`/`#[view]` + `PersistApp::serve_autonomous_model::<LedgerBook>(...)`):

- `POST /api/ledgers/`
- `GET /api/ledgers/`
- `GET /api/ledgers/:id`
- `DELETE /api/ledgers/:id`
- `GET /api/ledgers/:id/_audits`
- `POST /api/ledgers/:id/open_account`
- `POST /api/ledgers/:id/create_transfer`
- `GET /api/ledgers/:id/balance_report`
- `GET /api/ledgers/:id/account_balance?account_id=<id>`
- `POST /api/ledgers/:id/account_balance_body` with `{ "account_id": "..." }`

Create payload is constructor-based (`new(name)`), so `POST /api/ledgers` expects `{ "name": "..." }`.

Run:

```bash
cargo run --manifest-path examples/ledger_core/Cargo.toml
```

Test:

```bash
cargo test --manifest-path examples/ledger_core/Cargo.toml --offline
```
