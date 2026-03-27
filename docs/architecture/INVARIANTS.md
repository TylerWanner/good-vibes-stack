# Architecture Invariants

These are the non-negotiable rules of the good-vibes stack.

They are not implementation details.
They are the architectural bets that keep the system legible under pressure.

If a change violates one of these, the burden is on the change to justify itself.

---

## 1. Agency and orchestration are different layers

**OpenClaw is for judgment. Prefect is for execution.**

OpenClaw interprets intent, chooses a path, decides whether work should happen now, later, with approval, or not at all.

Prefect is not there to simulate judgment. It is there to run multi-step work reliably:
- retries
- scheduling
- concurrency control
- audit trail
- observability
- recovery

If a workflow requires durable execution semantics, it belongs in Prefect.
If a task requires interpretation, arbitration, or conversational judgment, it belongs in OpenClaw.

Do not collapse these layers just because one of them could technically do the other’s job.
That is how systems become muddy.

---

## 2. Durable product state does not live in chat or scheduler history

Chat is UI.
Scheduler history is telemetry.
Neither is durable product truth.

If something matters later, it must live in a durable state system appropriate to the domain:
- Postgres
- explicit config
- durable runtime state directories
- declared deployment config
- blocks or other scoped config stores

OpenClaw conversations are not the source of truth for workflow state.
Prefect run history is not the source of truth for application state.
Telegram messages are not the source of truth for approvals.

If recovery depends on reading chat logs or mentally reconstructing scheduler state, the architecture is wrong.

---

## 3. The database beats the workflow engine for domain truth

For domain state, **the database wins**.

Prefect knows what ran.
The database knows what is true.

That distinction matters whenever retries, cancellation, worker death, duplicate submissions, or partial failure enter the picture.

Examples:
- article ingest status belongs in the DB, not in Prefect state
- retry decisions should be driven from durable domain state, not flow-history folklore
- completion should mean domain state changed, not just that a run reached `Completed`

Schedulers are allowed to be noisy.
The domain model is not.

---

## 4. Mutating infrastructure actions must transit a bounded control plane

If an action can restart, stop, reconfigure, or otherwise change infrastructure, it must go through a narrow, explicit interface.

Preferred pattern:
- human or agent intent
- bounded control plane
- explicit authorization/policy
- auditable execution

Anti-pattern:
- shell access disguised as a feature
- ambient Docker socket power
- broad remote exec for “convenience”

This repo prefers constrained control planes over generalized execution because ambient power is where systems become un-debuggable and untrustworthy.

---

## 5. Ambient authority is a design smell

Broadly available secrets, filesystem access, and service power are not neutral conveniences.
They are latent incidents.

The preferred direction is always:
- more locality
- narrower authority
- workflow-scoped delivery
- explicit mounts
- explicit state dirs
- explicit policy

Not every part of the stack is there yet.
That does not weaken the principle.
It clarifies the migration direction.

A system that works only because everything can see everything is not robust. It is merely undiscovered fragility.

---

## 6. Shared workspaces must be explicit and honestly named

Shared state is allowed.
Implicit shared state is not.

If something is shared, it should be visible in the filesystem and in the architecture.
That is why workspace naming and mount boundaries matter.

The goal is not purity. The goal is legibility.
A system should reveal where collaboration and coupling exist instead of pretending they do not.

---

## 7. Personality is the editable surface; OpenClaw is the engine

Provision does not try to replace OpenClaw with a second agent runtime abstraction.

OpenClaw is the runtime engine.
Provision supplies the operating environment and the human-editable personality/config artifacts around it.

The intended editable surface is narrow and legible:
- `AGENTS.md`
- `SOUL.md`
- `IDENTITY.md`
- `HEARTBEAT.md`
- `TOOLS.md`
- `USER.md`
- repo-owned runtime config where necessary

This is deliberate.
The point is not to build a generic “agent builder.”
The point is to make agent behavior legible, reviewable, and operable.

---

## 8. Scripts are bootstrap/recovery tools, not the main control plane

Scripts are allowed to exist for:
- bootstrap
- restore
- migration
- one-time setup
- emergency repair

They are not the desired steady-state interface for normal system behavior.

If core operational behavior requires someone to remember the right shell incantation, the system is not finished.

The long-term direction is always toward:
- explicit APIs
- durable workflows
- bounded control surfaces
- reproducible init/restore paths

---

## 9. Every complexity increase must buy down real pain

This stack is opinionated because complexity is expensive.

A new layer, abstraction, queue, control plane, or configuration mechanism must justify itself in concrete terms:
- lower blast radius
- clearer ownership
- easier recovery
- less ambient access
- better observability
- stronger durability guarantees

We are not building a magical agent super-stack.
We are building a system that can be reasoned about when it is tired, broken, or under load.

---

## 10. The system should become more understandable as it grows, not less

Growth is not success if the mental model decays.

A strong system makes its boundaries clearer over time:
- more explicit authority
- clearer state ownership
- sharper control-plane roles
- more legible init and recovery

If new capabilities make the repo feel cleverer but harder to explain, that is architectural regression.

---

## The test

A good change makes at least one of these things better:
- authority becomes narrower
- truth becomes more durable
- recovery becomes easier
- execution becomes more observable
- boundaries become clearer

If it does none of those things, it is probably noise.
