# RustMemoDB Launch Kit

## Primary Showcase

Use `agentops_mission_control` as the flagship showcase.

Why this is the right choice:

- It demonstrates workflow-heavy backend behavior, not just CRUD.
- It shows generated REST, typed views, idempotent replay, audits, and restart durability in one story.
- It fits the strongest current wedge: Rust teams building AI control planes, internal operations surfaces, and reliability-heavy backend products.
- It feels contemporary and strategic, which makes it a better sales asset than a generic business app.

Keep `pulse_studio` as the secondary proof point for prospects who are less AI-native and more product-dashboard oriented.

## Positioning Statement

RustMemoDB helps Rust teams ship workflow-heavy backends with generated APIs, built-in idempotency, audits, and typed views from a single domain model.

## 7-Day Offer

Do not lead with "try our project."
Lead with this:

We will help your team ship the first real slice of a workflow-heavy backend in 7 days, using your domain model as the source of truth.

## What To Say

- "We compress the path from domain model to reliable backend."
- "You write business methods; RustMemoDB gives you generated API surfaces, audits, replay discipline, and typed views."
- "The goal is not a grand rewrite. The goal is one bounded service that becomes demoable and production-disciplined faster."

## What Not To Say

- "We replace Postgres everywhere."
- "We are just an embedded database."
- "We are mainly a benchmark story."
- "Trust us, the roadmap will cover the hard parts later."

## Default Demo Order

1. Show the `agentops_mission_control` model.
2. Show how little shell code exists in `main.rs`.
3. Execute one command with `Idempotency-Key`.
4. Replay the same command and show identical result without duplicate effects.
5. Fetch `_audits`.
6. Fetch `_openapi.json`.
7. Fetch one typed view.
8. Restart and show durable reopen.

## Commercial Framing

The commercial entry point is:

- one bounded service
- one technical champion
- one 30-minute scoping call
- one 7-day pilot
- one demo-ready slice at the end

The right outcome is not "they tried the repo."
The right outcome is "they want the second slice built the same way."
