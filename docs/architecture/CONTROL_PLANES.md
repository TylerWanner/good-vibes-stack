# Control Planes and Authority Boundaries

This stack contains multiple systems that can observe, decide, or cause side effects.
Therefore, it's important to prevent their authority from blurring together.

This document defines which plane owns which kind of power.

---

## The planes

### 1. OpenClaw — the interactive judgment plane

OpenClaw is the system that faces intent.
It is where interpretation happens.
It is where ambiguity is resolved.
It is where a human can say, in normal language, what they want, and the system decides how to route that request.

OpenClaw is responsible for:
- interpreting intent
- deciding which subsystem should act
- deciding whether work is immediate, deferred, scheduled, approval-gated, or refused
- coordinating agents and tools
- serving as the interactive control surface

OpenClaw is **not** responsible for being the durable execution substrate for long-running, multi-step, auditable workflows.

If work must survive interruption, be retried, be scheduled, or be observable as a durable job, OpenClaw should hand it off.

---

### 2. Prefect — the durable workflow execution plane

Prefect exists to make multi-step work reliable.

Prefect is responsible for:
- retries
- scheduling
- fan-out/fan-in execution
- concurrency limits
- durable run visibility
- structured execution of jobs that should survive beyond one interactive turn

Prefect is **not** the judgment layer.
It should not become the place where product philosophy or conversational reasoning hides.

Prefect runs the play.
It does not decide the strategy.

---

### 3. Application services — the domain planes

Application services own their domain semantics. In this stack, the Second Brain is the primary example: it owns the full knowledge lifecycle — ingest, fetch, analyze, score, store, retrieve. It has its own API surface, its own domain state in Postgres, and its own rules about what constitutes valid content and how it should be processed.

Application services are responsible for:
- domain rules and state transitions
- validation inside their domain
- stable APIs for their part of the system

They are **not** supposed to become generic orchestration layers. The Second Brain doesn't schedule its own retries — it exposes domain state and lets Prefect handle execution. The boundary matters.

---

### 4. Bounded infrastructure control planes — mutation with guardrails

Example: safe-docker — a narrow API that allows specific lifecycle operations (restart, status, logs) while blocking everything else (exec, run, arbitrary compose commands).

These systems exist because infrastructure mutation is too dangerous to expose as ambient shell or raw Docker access.

They are responsible for:
- narrow operational actions
- policy-bounded mutation
- auditable control
- reducing blast radius relative to direct host-level power

They are **not** general remote execution systems.
They are not a hidden backdoor for arbitrary code execution.

If a tool can do everything, it is not a bounded control plane. It is just power with nicer branding.

---

### 5. Scripts — bootstrap and repair

Scripts exist for setup and recovery. `init.sh` is idempotent and safe to re-run, but it is not part of normal operations — once the stack is running, everything is driven through the API, control planes, or Prefect flows.

The only operations that legitimately require going outside the system are those that **introduce new capabilities or code changes** to the components themselves — specifically, building new images. A new flow, a dependency update, a service change: these require a `docker compose build` and are intentionally human-gated. Everything else the stack can handle itself.

If something requires a script to do what an API or control plane should handle, that's the gap to close — not a reason to normalize the script.

---

## Authority boundaries

### Boundary A: OpenClaw → Prefect

OpenClaw may decide that a job should run.
Prefect should own the durable execution of that job.

**Rule:** when work needs retries, scheduling, fan-out, or durable observability, route to Prefect.

Do not let the agent session become a shadow workflow engine.

---

### Boundary B: Prefect → application/domain state

Prefect may execute domain work.
It does not own the domain truth.

**Rule:** Prefect run state is execution telemetry. Domain state lives in the domain’s durable store.

---

### Boundary C: OpenClaw / Prefect → infrastructure mutation

Neither OpenClaw nor Prefect should directly exercise broad host power if a bounded control plane can mediate it.

**Rule:** mutating infrastructure actions should transit explicit, constrained APIs rather than ambient shell/socket power.

---

### Boundary D: scripts → everything else

Scripts may initialize or repair the system.
They should not quietly become the hidden main interface.

**Rule:** if a script becomes a routine dependency of normal operations, it should either be promoted into a proper control plane or its capability should be made declarative.

---

### Boundary E: agent → flow code

The agent may write and propose flow code. It may not deploy it unilaterally.

Flow code is baked into the worker image at build time — not mounted from a live volume. This means a code change alone does not take effect. The worker must be rebuilt and recreated.

**Rule:** deploying new or modified flow code requires a human-initiated image build. The agent proposes; the build is the gate.

`docker compose build prefect-worker` is a `dangerous` operation in safe-docker — it requires explicit opt-in. This is intentional. The build step is the human-in-the-loop checkpoint between agent-authored code and running code.

**Trust assumption to name explicitly:** if safe-docker allows the build operation, and the repo is mounted into the build context, then an agent with write access to the repo has a path to arbitrary code execution — write code, trigger build, code runs. The build gate slows this down and requires human opt-in, but it does not eliminate the surface. In a production deployment where the agent should not have this capability, the repo mount should be scoped to only the directories the agent legitimately needs to write to — not the entire codebase. This is an open design question for hardened deployments.

---

## Escalation rule

When deciding where a behavior belongs, choose the narrowest plane that can own it correctly.

Use this order of preference:

1. domain service/API, if the behavior is domain-native
2. bounded control plane, if the behavior is operational mutation
3. Prefect, if the behavior is durable workflow execution
4. OpenClaw, if the behavior is judgment/routing/interactive control
5. script, if the behavior is bootstrap/recovery and not yet consolidated

That ordering is not absolute, but it reflects the design bias of this repo:
**narrow authority beats broad convenience.**

---

## Anti-patterns

These are signs that authority boundaries are rotting:

- OpenClaw sessions carrying durable workflow state
- Prefect flows encoding conversational judgment
- application services acting as generic control planes
- shell scripts becoming required daily operations
- raw Docker or host power available where a bounded control plane should exist
- approvals represented only in chat messages with no durable state
- multiple ways to perform the same mutating action with different audit properties

Any of these can work for a while.
All of them get expensive under failure.

---

## Desired end state

A healthy stack looks like this:

- humans express intent through OpenClaw
- OpenClaw routes work to the correct plane
- Prefect reliably executes durable jobs
- application services own domain semantics
- bounded control planes own infrastructure mutation
- scripts exist mostly for init, restore, and unusual repair
- durable state lives in durable systems, not in chats or scheduler folklore

That is the point.
Not maximal flexibility.
Legible authority.
