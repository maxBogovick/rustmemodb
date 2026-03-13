# RFC: Autonomous DSL v2 (Compact and Powerful)

Status: Draft  
Owner: Product DX / Persist  
Updated: 2026-02-22

## 1. Purpose

Define an application DSL that is:

1. shorter than current `Autonomous + expose_rest` Rust form,
2. powerful enough for real business domains,
3. zero-leak in default path (no persistence internals in app code),
4. compiled to existing stable `rustmemodb::prelude::dx::*` contracts.

This RFC replaces the verbose v1 draft syntax with a compact v2 syntax.

## 2. Review of v1 Syntax (What Was Wrong)

v1 was correct semantically but too heavy ergonomically.

Main issues:

1. Too many keywords per intent (`command/query/view`, `input:`, `mode:`).
2. Field declarations too verbose (`required`, `default`) compared to modern DSLs.
3. REST exposure and policies were separated from aggregate context, increasing mental hops.
4. Error mapping syntax was noisy (`=> http ... code ...`) for common cases.
5. It did not feel materially shorter than Rust macro-based code.

Conclusion: v1 was implementable, but not "wow DX".

## 3. v2 Design Goals

1. Express common domain contracts with minimal tokens.
2. Make "simple things one-liner", keep advanced options available.
3. Keep business language first: aggregate, command, query, error, invariant.
4. Keep runtime guarantees: atomicity, idempotency, deterministic behavior, typed errors.
5. Allow direct future path to "DSL-first projects", not only documentation sugar.

## 4. v2 Core Syntax

## 4.1 Top-Level Shape

```dsl
app LedgerCore @rest("/api/ledgers") {
  aggregate LedgerBook {
    name: text!
    accounts: [LedgerAccount] = []
    transactions: [LedgerTransaction] = []

    !open_account(OpenAccountInput) -> LedgerAccount
    !create_transfer(CreateTransferInput) -> LedgerTransaction

    ?balance_report() -> LedgerBalanceReport
    ?account_balance(AccountBalanceQuery) -> i64
    ?account_balance_body(AccountBalanceQuery) -> i64 @body
  }

  errors LedgerDomainError {
    AccountNotFound(account_id:text): 404/"account_not_found"
    InvalidAmount(field:text): 422/"invalid_amount"
    InsufficientFunds(account_id:text, available_minor:i64, required_minor:i64): 409/"insufficient_funds"
  }

  defaults {
    idempotency on
    audit auto
    retries write_conflict(3, 5ms, 25ms)
  }
}
```

## 4.2 Compact Tokens

1. `!name(...)` means command (state mutation).
2. `?name(...)` means query/view (read).
3. `@body` on `?` switches binding from query params to request body.
4. `text!` means required.
5. `text?` means optional.
6. `= ...` means default value.

## 4.3 Type System Shorthand

1. Scalars: `text`, `i64`, `bool`, `uuid`, `datetime`, `json`.
2. Optional: `T?`.
3. List: `[T]`.
4. Map: `{text:T}`.

Examples:

```dsl
owner_name: text!
note: text?
tags: [text] = []
metadata: {text:json} = {}
```

## 4.4 Smart Defaults (No Boilerplate by Default)

1. Commands are idempotent by default.
2. Single DTO argument is direct payload binding.
3. Query binding defaults to URL params.
4. Audit is automatic (`_audits` route available).
5. OpenAPI route is automatic (`/_openapi.json`).
6. Standard CRUD for aggregate root is automatic unless disabled explicitly.

Opt-out examples:

```dsl
!enqueue_job(EnqueueInput) -> JobTicket @non_idempotent
?expensive_report(ReportQuery) -> Report @body
```

## 4.5 Inline Business Guards (Optional, v2+)

For compact business invariants without manual Rust glue:

```dsl
!create_transfer(CreateTransferInput) -> LedgerTransaction {
  require amount_minor > 0 -> InvalidAmount("amount_minor")
  require from_account_id != to_account_id -> SameAccountTransfer()
}
```

This stays optional; complex logic can still be bound to Rust handlers.

## 4.6 Schema-Inference Friendly Declarations

To minimize manual DTO writing, DSL supports shape-based declaration hooks:

```dsl
shape OpenAccountInput <- json {
  "owner_name": "Main Wallet",
  "currency": "USD",
  "opening_balance_minor": 10000,
  "note": null
}
```

Intended backend: `unistructgen` pipeline for type generation.

## 5. Full LedgerCore v2 Example

```dsl
app LedgerCore @rest("/api/ledgers") {
  shape OpenAccountInput <- json {
    "owner_name": "Main Wallet",
    "currency": "USD",
    "opening_balance_minor": 10000,
    "note": null
  }

  shape CreateTransferInput <- json {
    "from_account_id": "acc_from",
    "to_account_id": "acc_to",
    "amount_minor": 1500,
    "to_amount_minor": null,
    "note": null
  }

  shape AccountBalanceQuery <- json {
    "account_id": "acc_1"
  }

  aggregate LedgerBook {
    name: text!
    accounts: [LedgerAccount] = []
    transactions: [LedgerTransaction] = []

    !open_account(OpenAccountInput) -> LedgerAccount
    !create_transfer(CreateTransferInput) -> LedgerTransaction {
      require amount_minor > 0 -> InvalidAmount("amount_minor")
      require from_account_id != to_account_id -> SameAccountTransfer()
    }

    ?balance_report() -> LedgerBalanceReport
    ?account_balance(AccountBalanceQuery) -> i64
    ?account_balance_body(AccountBalanceQuery) -> i64 @body
  }

  errors LedgerDomainError {
    AccountNotFound(account_id:text): 404/"account_not_found"
    InvalidCurrency(currency:text): 422/"invalid_currency"
    InvalidAmount(field:text): 422/"invalid_amount"
    SameAccountTransfer(): 422/"same_account_transfer"
    InsufficientFunds(account_id:text, available_minor:i64, required_minor:i64): 409/"insufficient_funds"
  }

  defaults {
    idempotency on
    audit auto
    retries write_conflict(3, 5ms, 25ms)
  }
}
```

## 6. Before/After (Expressiveness)

Old-style declaration (v1 idea):

```dsl
command create_transfer(input: CreateTransferInput) -> LedgerTransaction;
query account_balance(input: AccountBalanceQuery) -> Int64;
view account_balance_body(input: AccountBalanceQuery, mode: body) -> Int64;
```

New compact v2:

```dsl
!create_transfer(CreateTransferInput) -> LedgerTransaction
?account_balance(AccountBalanceQuery) -> i64
?account_balance_body(AccountBalanceQuery) -> i64 @body
```

Net effect: fewer keywords, less punctuation, same semantics.

## 7. Semantic Contract (Must Stay True)

1. `!` operations are atomic state mutations.
2. `?` operations are read-only.
3. Idempotency replay for `!` is on by default.
4. Generated HTTP and OpenAPI must be deterministic from DSL.
5. Error variants map to stable machine codes and HTTP statuses.
6. Default app path must not leak `session/tx/repository/store` concepts.

## 8. Mapping to Existing RustMemoDB Runtime

1. `aggregate` -> `#[derive(Autonomous)] + #[persist_model(...)]`.
2. `!command` -> `#[command]`.
3. `?query` -> `#[query]` or `#[view]` depending on binding mode.
4. `errors ... : status/"code"` -> `#[derive(ApiError)]` + `#[api_error(...)]`.
5. `@rest("/api/...")` -> generated mount using `PersistApp::serve_autonomous_model::<Model>(...)`.
6. `defaults.idempotency on` -> generated command handlers with replay support.
7. `shape ... <- json` -> generated Rust DTO types via `unistructgen` integration.

## 9. Grammar Sketch (EBNF, v2)

```ebnf
app_decl        = "app", IDENT, rest_attr?, "{", app_item*, "}" ;
rest_attr       = "@rest", "(", STRING, ")" ;

app_item        = shape_decl | aggregate_decl | errors_decl | defaults_decl ;

shape_decl      = "shape", IDENT, "<-", "json", json_block ;
json_block      = "{", JSON_TEXT, "}" ;

aggregate_decl  = "aggregate", IDENT, "{", field_decl*, op_decl*, "}" ;
field_decl      = IDENT, ":", type_ref, default_clause?, ";"? ;
type_ref        = base_type, ("!" | "?" )?
                | "[", type_ref, "]"
                | "{", "text", ":", type_ref, "}" ;
default_clause  = "=", literal ;

op_decl         = cmd_decl | qry_decl ;
cmd_decl        = "!", IDENT, "(", type_ref_or_ident?, ")", ret_clause?, op_attr*, guard_block? ;
qry_decl        = "?", IDENT, "(", type_ref_or_ident?, ")", ret_clause?, op_attr* ;
ret_clause      = "->", type_ref_or_ident ;
op_attr         = "@body" | "@non_idempotent" ;

guard_block     = "{", guard_stmt+, "}" ;
guard_stmt      = "require", expr, "->", IDENT, "(", arg_list?, ")" ;

errors_decl     = "errors", IDENT, "{", error_variant+, "}" ;
error_variant   = IDENT, "(", fields?, ")", ":", INT, "/", STRING ;

defaults_decl   = "defaults", "{", default_stmt+, "}" ;
default_stmt    = "idempotency", ("on"|"off")
                | "audit", ("auto"|"off")
                | "retries", retry_policy ;
```

## 10. Implementation Plan

### Phase A: Spec Freeze

1. Freeze token set (`!`, `?`, `@body`, `@non_idempotent`, `!/?` nullability).
2. Freeze scalar aliases (`text`, `i64`, `uuid`, etc.).
3. Freeze error mapping format (`status/"code"`).

Acceptance:

1. No unresolved syntax ambiguities in parser test corpus.

### Phase B: Parser + AST

1. Build lexer/parser for v2 grammar.
2. Build semantic validator (duplicate names, undefined types, invalid defaults).
3. Build source spans for diagnostics.

Acceptance:

1. Parser and semantic tests cover happy-path and error-path cases.

### Phase C: Codegen to Existing DX API

1. Generate model Rust code to `prelude::dx` contracts.
2. Generate exposed REST + OpenAPI through current runtime.
3. Generate error mapping glue and policy settings.

Acceptance:

1. `ledger_core` and `agile_board` generated outputs pass existing integration tests.

### Phase D: Shape + Validation Powerups

1. `shape <- json` integrated via `unistructgen`.
2. `require ... -> Error(...)` guards compile to typed validation paths.
3. Validation maps to stable `422` responses.

Acceptance:

1. Manual DTO and `normalize_*` boilerplate reduced in showcase examples.

### Phase E: Tooling

1. CLI commands:
   - `persist dsl check`
   - `persist dsl build`
   - `persist dsl fmt`
2. Source maps from generated Rust to DSL lines.
3. Fast incremental builds.

Acceptance:

1. CI can gate on `persist dsl check`.
2. Diagnostics include exact line/column and fix hints.

## 11. Product Goals and Success Criteria

Primary goals:

1. Enable building apps by writing domain intent only.
2. Make REST + persistence behavior mostly declarative.
3. Keep migration path to raw Rust APIs safe and explicit.

Measurable success criteria:

1. DSL aggregate definition requires substantially fewer lines than equivalent Rust model + attributes.
2. Generated showcase apps contain no manual persistence plumbing layers.
3. Idempotency, audit route, and error mapping work by default in generated output.
4. Existing showcase test suites stay green after migration to DSL-generated code.

## 12. Immediate Next Steps

1. Freeze this v2 syntax after final naming pass.
2. Implement parser + AST (Phase B).
3. Generate LedgerCore first as the reference target.
4. Use `examples/ledger_core` HTTP tests as hard acceptance gate for first end-to-end compilation.
