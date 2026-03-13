# LedgerCore Example

LedgerCore is a command-heavy financial workflow example built on the high-level DX surface: `rustmemodb::prelude::dx::*`.

Related reading:

- [`docs/src/guides/contracts_audits_and_idempotency.md`](../../docs/src/guides/contracts_audits_and_idempotency.md)
- [`docs/src/playbooks/reliability_you_can_demo.md`](../../docs/src/playbooks/reliability_you_can_demo.md)
- [`docs/src/briefings/how_to_de_risk_the_first_launch.md`](../../docs/src/briefings/how_to_de_risk_the_first_launch.md)

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
- Generated list endpoint query DSL (`page`, `per_page`, `sort`, `field`, `field__op`).

HTTP endpoints (generated automatically via `#[expose_rest]` + `#[command]`/`#[query]`/`#[view]` + `serve_domain!(...)`):

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

List query examples:

```bash
BASE=http://127.0.0.1:3001

# sort + pagination
curl -s "$BASE/api/ledgers?sort=name&page=1&per_page=2" | jq

# contains filter
curl -s "$BASE/api/ledgers?name__contains=alpha" | jq
```

Runtime behavior is validated in
`tests/http_api.rs` (`generated_router_supports_list_query_params`).

Run:

```bash
cargo run --manifest-path examples/ledger_core/Cargo.toml
```

Test:

```bash
cargo test --manifest-path examples/ledger_core/Cargo.toml --offline
```
