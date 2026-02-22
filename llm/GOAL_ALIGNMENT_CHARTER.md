# Goal Alignment Charter: Autonomous Structures and Invisible Persistence

Status: ACTIVE
Last Updated: 2026-02-15
Primary Owner: Product and Architecture
Applies To: persist module, examples, education track, API surface, runtime architecture

## 1. Purpose of This Charter

This document is a permanent anti-drift anchor.
Its role is to ensure every design and implementation decision stays aligned with the core vision:

"The best database is the one the application developer does not know exists."

If a proposal improves internals but increases application-layer persistence thinking, it violates this charter.

## 2. Canonical Source Set

The following documents define intent and scope and must remain consistent:

- `/Users/maxim/RustroverProjects/rustmemodb/llm/VISION_AUTONOMOUS_STRUCTURES.md`
- `/Users/maxim/RustroverProjects/rustmemodb/PERSIST_ROADMAP.md`
- `/Users/maxim/RustroverProjects/rustmemodb/PERSIST_PROGRESS.md`
- `/Users/maxim/RustroverProjects/rustmemodb/docs/persist/RFC_COMMAND_FIRST_PERSIST.md`
- `/Users/maxim/RustroverProjects/rustmemodb/llm/PROPOSAL_INVISIBLE_PERSISTENCE.md`

If conflicts appear, this precedence applies:

1. Vision
2. RFC contract
3. This charter
4. Roadmap and implementation plan
5. Examples and education materials

## 3. Core Product Thesis

The product is not a better ORM.
The product is a new application programming model where persistence is intrinsic to data structures.

Practical meaning:

- Business code manipulates domain structures, not storage adapters.
- Save/load/index/constraint/recovery logic is not handwritten per app.
- Database concerns are infrastructure details, not application responsibilities.

## 4. Non-Negotiable Outcomes

These outcomes are mandatory.

1. Single-world mental model
- Developers reason about domain state only.
- They do not reason about "memory vs disk" split in normal flows.

2. Autonomous structures
- Structures self-preserve, self-organize, and self-validate.

3. Layer deletion
- Repository boilerplate, storage mapping glue, and manual persistence orchestration are removed or generated.

4. Safety parity or better
- Simplicity must not sacrifice uniqueness, consistency, idempotency, recoverability, or observability.

5. Contract-first parity
- Product implementation and classic implementation expose identical external API contracts in A/B lessons.

## 5. Target User Experience

## 5.1 Application Developer Experience

A developer should be able to:

- define domain model and invariants declaratively,
- issue commands and queries through typed APIs,
- get persistence, constraints, audit, and recovery automatically,
- avoid direct usage of storage lifecycle primitives.

## 5.2 Education Experience

A learner should observe:

- same user value in Part A and Part B,
- dramatically less persistence boilerplate in Part B,
- no hidden complexity moved to another application layer.

## 6. In-Scope and Out-of-Scope

## 6.1 In-Scope

- Command-first domain mutation APIs.
- Declarative constraints and indices.
- Aggregate-level and multi-collection atomic operations.
- Built-in audit and outbox patterns.
- Declarative query and projection APIs.
- Generated app-facing adapters for web APIs.

## 6.2 Out-of-Scope

- Forcing users to write framework-specific storage classes for basic CRUD.
- Replacing one handwritten repository with another handwritten "persist store".
- Requiring manual synchronization code between domain and persistence metadata.
- Requiring app teams to learn internal journal/snapshot mechanics for normal work.

## 7. Product Promises

These are promises to users and to ourselves.

1. No persistence leakage in business logic.
2. No manual save choreography in handlers and services.
3. No manual uniqueness infrastructure in application code.
4. No manual cross-collection compensation flows for common use cases.
5. Predictable conflict and idempotency behavior via stable error model.
6. Transparent runtime observability without app boilerplate.

## 8. Drift Indicators (Early Warning)

A design is drifting if one or more are true:

- Application code manually opens and holds multiple managed collections for one use case.
- Application code introduces storage mutex orchestration for domain operations.
- Application code writes custom uniqueness claim collections.
- Application code implements manual rollback compensation after partial writes.
- New endpoints require new repository methods that only wrap persistence plumbing.
- Part B lesson code size is not materially smaller than Part A for equivalent value.

## 9. Hard Acceptance Criteria

## 9.1 API Layer Acceptance

For standard CRUD plus command use cases:

- No direct `ManagedPersistVec` usage in handlers/services.
- No direct `PersistSession` usage in handlers/services.
- No manual snapshot or restore calls in request paths.

## 9.2 Domain Layer Acceptance

- Domain invariants are declared once.
- Domain commands are first-class and typed.
- Domain validation errors map to stable public error classes.

## 9.3 Storage Behavior Acceptance

- Atomicity for multi-entity operations used in common business flows.
- Declarative uniqueness and index enforcement without app-side claim tables.
- Deterministic replay and idempotent command execution where declared.

## 9.4 Operational Acceptance

- Restart recovery works with zero app restoration code.
- Outbox records and dispatch status are inspectable.
- Projection lag and durability lag are measurable.

## 10. Quantitative Success Metrics

Target metrics for product-mode implementations:

1. Boilerplate reduction
- Part B persistence-specific LOC <= 35 percent of Part A persistence-specific LOC.

2. Storage leakage
- Count of app-layer direct persistence primitives in normal request flow: 0.

3. Constraint plumbing
- Count of app-defined technical claim/index entities for common uniqueness cases: 0.

4. Endpoint implementation speed
- New business endpoint time reduced by at least 50 percent relative to Part A baseline.

5. Reliability parity
- Contract test parity at 100 percent for A/B implementations.

## 11. Design Decision Gate

Every architecture proposal must answer all questions below before approval.

1. Does this reduce persistence thinking in app code?
2. Does this remove, not move, boilerplate?
3. Does this improve or preserve safety properties?
4. Can this be expressed declaratively rather than imperatively?
5. Does this reduce the size of educational Part B code?
6. Is there a migration path from current API?
7. Is the behavior testable through contract and integration tests?

If any answer is "no" without a documented exception, reject or redesign.

## 12. Exception Policy

Exceptions are allowed only when all are true:

- The capability is advanced and not part of standard application flow.
- The exception is isolated behind opt-in API surface.
- The default path remains invisible-persistence first.
- Documentation explicitly marks it as advanced escape hatch.

## 13. Education Track Guardrails

For each lesson:

1. User value statement is explicit.
2. Part A and Part B deliver the same external contract.
3. Part B must remove at least one entire category of persistence boilerplate.
4. Shared contract tests are the source of truth for parity.
5. Cliffhanger must point to product capability growth, not tutorial complexity growth.

## 14. Anti-Patterns to Eliminate

These patterns must be actively removed over time:

- Repository code that mostly forwards to persistence APIs.
- Manual lock orchestration around persistence containers.
- Manual envelope, audit, and outbox record creation in app code.
- Handwritten projection maintenance in request handlers.
- Cross-collection business transactions implemented as ad hoc compensation logic.

## 15. Strategic End State

The end state is reached when a product-mode application can be built with:

- domain models,
- declarative constraints,
- declarative command handlers,
- generated or framework-provided API adapters,

while all persistence mechanics remain internal to the persist runtime and generated interfaces.

## 16. Governance and Review Cadence

- This charter is reviewed every two weeks during roadmap planning.
- Any new feature touching persistence APIs must include a "charter alignment" note.
- Education lesson updates must include an explicit "boilerplate reduced" summary.

## 17. One-Line Litmus Test

If a junior developer building a CRUD endpoint must think about where and how to persist state,
we are not done.
