# PulseStudio: Modern Growth OS for RustMemoDB

`PulseStudio` is a modern product-style backend for creator/marketing teams:

1. workspace management,
2. channel onboarding,
3. campaign launch,
4. spend tracking with hard budget safety,
5. engagement events,
6. instant dashboard and channel/campaign analytics.

This project is intentionally chosen because it stresses exactly what usually hurts in backend development:

1. atomic business mutations,
2. idempotency on retries,
3. clean domain errors,
4. fast product iteration without DB-plumbing fatigue.

---

Related reading:

- [`docs/src/guides/pulse_studio_walkthrough.md`](../../docs/src/guides/pulse_studio_walkthrough.md)
- [`docs/src/playbooks/views_and_ai_control_planes.md`](../../docs/src/playbooks/views_and_ai_control_planes.md)
- [`docs/src/briefings/what_technical_buyers_need_to_believe.md`](../../docs/src/briefings/what_technical_buyers_need_to_believe.md)

## Why this is an ideal showcase for RustMemoDB

### Product-level pain points this example solves

1. **Budget safety is business-critical**:
`record_spend` must never partially apply.
It also must be blocked when campaign lifecycle status is not `running`.
Campaign launch is also rejected when the channel is inactive.
2. **Idempotency is mandatory**:
client retries must not duplicate writes.
3. **Read models are product-facing**:
`workspace_dashboard` and `channel_overview` should stay easy to evolve.
4. **Team velocity matters**:
new commands/queries should be added by writing domain logic, not infrastructure boilerplate.

### What RustMemoDB gives here

1. Generated REST from domain methods (`#[api]`).
2. Auto-mounted typed dashboard view via `#[api(views(PulseDashboard))]` (`GET /api/workspaces/:id/views/dashboard`).
3. Automatic idempotent replay for commands (`Idempotency-Key`).
4. Generated OpenAPI (`GET /api/workspaces/_openapi.json`).
5. Declarative error mapping (`#[derive(DomainError)]`).
6. Query/command request shape generated from method signatures (no hand-written API DTO layer).
7. Nested persistent state via `PersistJson<T>` without custom ORM mapping.
8. Domain invariants stay in one place (channel active guard, per-platform handle uniqueness, input constraints).
9. No manual projection cache fields in the model (`stats/recompute_*` are absent).
10. Generated list query DSL on `GET /api/workspaces` (`page`, `per_page`, `sort`, `field`, `field__op`).

---

## Part A: How this is usually built (classic approach)

In a typical stack (`axum + sqlx + postgres + migration tool`) this same product usually requires:

1. SQL schema + migrations,
2. DTO layer,
3. HTTP handlers,
4. service layer,
5. repository layer,
6. transaction orchestration and consistency checks,
7. idempotency storage + replay logic,
8. OpenAPI wiring,
9. error mapping glue.

### Typical migration (classic)

```sql
CREATE TABLE workspaces (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE campaigns (
  id UUID PRIMARY KEY,
  workspace_id UUID NOT NULL REFERENCES workspaces(id),
  title TEXT NOT NULL,
  budget_minor BIGINT NOT NULL,
  spent_minor BIGINT NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE activity_events (
  id UUID PRIMARY KEY,
  campaign_id UUID NOT NULL REFERENCES campaigns(id),
  event_type TEXT NOT NULL,
  amount_minor BIGINT,
  points BIGINT,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE command_idempotency (
  key TEXT PRIMARY KEY,
  route TEXT NOT NULL,
  response_status INT NOT NULL,
  response_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
```

### Typical spend mutation (classic)

```rust
pub async fn record_spend(
    &self,
    campaign_id: Uuid,
    amount_minor: i64,
) -> anyhow::Result<CampaignProgressDto> {
    let mut tx = self.pool.begin().await?;

    let campaign = sqlx::query_as!(
        CampaignRow,
        "SELECT id, budget_minor, spent_minor, status FROM campaigns WHERE id = $1 FOR UPDATE",
        campaign_id
    )
    .fetch_one(&mut *tx)
    .await?;

    let attempted = campaign.spent_minor + amount_minor;
    if attempted > campaign.budget_minor {
        tx.rollback().await?;
        return Err(DomainError::BudgetExceeded {
            campaign_id,
            budget_minor: campaign.budget_minor,
            attempted_spent_minor: attempted,
        }
        .into());
    }

    sqlx::query!(
        "UPDATE campaigns SET spent_minor = $1, status = $2 WHERE id = $3",
        attempted,
        if attempted == campaign.budget_minor { "completed" } else { &campaign.status },
        campaign_id
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        "INSERT INTO activity_events (id, campaign_id, event_type, amount_minor, created_at)
         VALUES ($1, $2, 'spend', $3, now())",
        Uuid::new_v4(),
        campaign_id,
        amount_minor
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(self.repo.load_campaign_progress(campaign_id).await?)
}
```

### Typical handler idempotency (classic)

```rust
pub async fn record_spend_handler(
    State(app): State<AppState>,
    headers: HeaderMap,
    Path(campaign_id): Path<Uuid>,
    Json(body): Json<RecordSpendRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::bad_request("missing idempotency key"))?;

    if let Some(replayed) = app.idempotency_repo.find(key).await? {
        return Ok((StatusCode::from_u16(replayed.status)?, Json(replayed.body)));
    }

    let result = app.service.record_spend(campaign_id, body.amount_minor).await?;
    app.idempotency_repo.save(key, 200, &result).await?;
    Ok((StatusCode::OK, Json(result)))
}
```

The business rule is mixed with transaction, SQL, lock semantics, and replay storage plumbing.

---

## Part B: Same product with RustMemoDB (this example)

In `PulseStudio`, the developer mainly writes domain behavior.

```rust
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
    #[api_error(status = 409, code = "budget_exceeded")]
    BudgetExceeded { campaign_id: String, budget_minor: i64, attempted_spent_minor: i64 },
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
}

#[api]
impl PulseWorkspace {
    #[command]
    pub fn record_spend(
        &mut self,
        campaign_id: String,
        amount_minor: i64,
    ) -> Result<PulseCampaignProgress, PulseStudioError> {
        // business rule first
        // persistence + REST + idempotency plumbing is generated
    }

    #[query]
    pub fn workspace_dashboard(&self) -> PulseDashboard {
        // read model built directly from domain state
    }
}
```

And bootstrap stays minimal:

```rust
let app = PersistApp::open_auto(data_dir).await?;
let router = rustmemodb::serve_domain!(app, PulseWorkspace, "workspaces")?;
```

---

## Concrete value for product teams

1. **Faster feature delivery**:
new command/query = new domain method.
2. **Safer money-like flows**:
spend mutations are consistent and tested without manual tx ceremony.
3. **Lower cognitive load**:
engineers think in product invariants, not DB choreography.
4. **Better demoability**:
OpenAPI, idempotency, and error contracts are visible out of the box.

---

## Run

```bash
cargo run --manifest-path examples/pulse_studio/Cargo.toml
```

Defaults:

1. address: `127.0.0.1:3022`
2. data dir: `./pulse_studio_data`

---

## Test quality gate

```bash
cargo test --manifest-path examples/pulse_studio/Cargo.toml --offline
```

HTTP tests verify:

1. end-to-end product flow,
2. validation (`422`) behavior,
3. idempotent replay with `Idempotency-Key`,
4. conflict mapping (`409 budget_exceeded`) without partial writes,
5. generated OpenAPI contract and idempotency header docs.
6. generated list query DSL behavior for sorting/filtering/pagination and validation.

---

## Suggested demo script (for live presentation)

1. `POST /api/workspaces` create workspace.
2. `POST /api/workspaces/:id/add_channel` with `Idempotency-Key` twice, show no duplicate.
3. `POST /api/workspaces/:id/launch_campaign`.
4. `POST /api/workspaces/:id/record_spend` success.
5. `POST /api/workspaces/:id/record_spend` overflow, show `409 budget_exceeded`.
6. `GET /api/workspaces/:id/workspace_dashboard` show stable analytics.
7. `GET /api/workspaces/:id/views/dashboard` show auto-mounted typed PersistView endpoint.
8. `GET /api/workspaces/_openapi.json` show generated contract.
9. `GET /api/workspaces?sort=name&page=1&per_page=2` show generated list query DSL.

This sequence clearly demonstrates why this product is not "code generator for CRUD", but an execution model where infrastructure concerns are pushed down and business intent stays on top.
