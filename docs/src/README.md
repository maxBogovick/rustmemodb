# RustMemoDB

RustMemoDB is an embedded persistence and runtime toolkit for Rust applications that want business-logic-first APIs without starting from a separate database service.

The recommended product surface today is `rustmemodb::prelude::dx::*`.
This book is written around the APIs that are currently exercised by examples and tests in this repository.

## What You Can Rely On Today

- `#[domain]` for persisted domain models.
- `#[api]` and `#[expose_rest]` for generated REST contracts from model methods.
- `PersistApp::open_auto(...)` for opening durable embedded storage.
- `serve_domain!(...)` for mounting generated routers.
- `PersistJson<T>` for nested aggregate state without local wrapper boilerplate.
- Built-in audit endpoints, generated OpenAPI, typed views, and list query DSL.
- `AgentSessionRuntime` and `AgentWorkflowExecutor` for AI session memory and replay flows.

## Who This Fits

- Rust teams shipping single-binary services, edge apps, or local-first tools.
- Product codebases that want the domain model, persistence, and HTTP contract to stay close together.
- Teams that value fast iteration and fewer infrastructure dependencies during the first product phase.

## Who This Does Not Fit

- Teams whose primary requirement is a shared external SQL service for many unrelated applications.
- SQL-first analytics and warehouse workloads.
- Systems where cross-language writes to the same store are the main integration model.

## Recommended First Path

1. Read [Quickstart](quickstart.md).
2. Skim [Architecture Overview](architecture.md).
3. Read [Tradeoffs and Limits](tradeoffs.md).
4. Use [Proof and Examples](proof_and_examples.md) as the trust anchor.
5. Then go deeper into the feature pages.
6. Read the Product Playbooks if you want to see what kinds of products RustMemoDB can help you ship quickly and credibly.
7. Use the Hands-On Guides when you want concrete walkthroughs, exact demo flows, and product-facing patterns such as typed views and query DSL.
8. Read the Technical Buyer Briefings when you need to pitch RustMemoDB internally, win an architecture review, or turn a demo into a credible pilot.

This documentation intentionally avoids speculative API stories.
If a page recommends a surface, it should already exist in `examples/` or `tests/`.
