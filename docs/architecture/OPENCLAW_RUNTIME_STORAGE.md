# OpenClaw Runtime Storage

This repo separates OpenClaw state into two categories:

1. **runtime-owned substrate** — durable state OpenClaw manages itself
2. **human-managed overlays** — files operators are expected to read, edit, or hand off

---

## Default layout

The example agent now mounts:

- `openclaw-runtime` named volume → `/home/node/.openclaw`
- `./agents/example` → `/home/node/.openclaw/workspace` (read-only)
- `./shared-workspace` → `/home/node/.openclaw/workspace/shared-workspace`
- repo root → `/workspace`

### Why this split exists

A full-tree bind mount for `/home/node/.openclaw` is tempting, but it mixes together:
- runtime session state
- caches
- auth/runtime substrate
- human-edited workspace files

That makes permissions and portability harder than they need to be.

The named-volume + overlay pattern keeps the broad runtime substrate durable without pretending every internal file should be repo-owned.

---

## What belongs where

### Named volume: `openclaw-runtime`
Use for:
- OpenClaw internal runtime state
- session/cached data
- other durable `.openclaw` files not meant for routine manual editing

### Bind mount: `./agents/example`
Use for:
- persona/config/workspace files the operator edits intentionally
- startup-context files like `AGENTS.md`, `SOUL.md`, `TOOLS.md`, `USER.md`
- repo-owned docs and notes that should travel with the repo

### Bind mount: `./shared-workspace`
Use for:
- explicit handoff files
- scratch artifacts you want visible from both the host and the agent workspace
- shared outputs that should not be mixed into the curated agent workspace files

---

## Practical rule

- If humans edit it often, make it an explicit bind mount.
- If OpenClaw owns it operationally, keep it in the named volume.
- If it is meant for handoff, keep it in `shared-workspace` instead of smearing it into random runtime paths.
