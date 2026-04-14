# OpenClaw Runtime Storage

This repo separates OpenClaw state into two categories:

1. **runtime-owned substrate** — durable state OpenClaw manages itself
2. **human-managed overlays** — files operators are expected to read, edit, or hand off

---

## Default layout

The example agent now mounts:

- `openclaw-runtime` named volume → `/home/node/.openclaw`
- `./agents/example/openclaw-data/workspace` → `/home/node/.openclaw/workspace`
- `./agents/example/openclaw-data/media` → `/home/node/.openclaw/media`
- `./agents/example/openclaw-data/agents` → `/home/node/.openclaw/agents`
- `./agents/example/openclaw-data/memory` → `/home/node/.openclaw/memory`
- `./agents/example/openclaw-data/tmp` → `/home/node/.openclaw/tmp`
- `./shared-workspace` → `/home/node/.openclaw/workspace/shared-workspace`
- read-only persona/config overlays into `/home/node/.openclaw/workspace`
- repo root → `/workspace`

Think of this as a five-part filesystem contract:

1. **runtime substrate** — broad `.openclaw` state OpenClaw owns
2. **durable submounts** — explicit workspace/media/agents/memory/tmp paths operators may want to inspect or preserve
3. **curated startup overlays** — role-defining files mounted read-only into the workspace
4. **handoff space** — explicit shared artifacts that should be visible to both host and agent
5. **repo mount** — the larger project tree, mounted separately so workspace identity stays clear

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

### Durable submounts: `./agents/example/openclaw-data/*`
Use for:
- the workspace/media/agents/memory/tmp paths that sit under `.openclaw`
- data you may want visible on the host without making the whole runtime tree a bind mount
- explicit persistence boundaries for common operational paths

These keep the broad runtime volume from becoming an opaque blob while still avoiding a full-tree bind mount.

### Read-only overlays: `./agents/example/*.md` + config
Use for:
- startup-context files like `AGENTS.md`, `SOUL.md`, `TOOLS.md`, `USER.md`
- role/persona/config files that should be edited intentionally from the repo
- `openclaw.json`

These are the curated startup overlays, not a dumping ground for every artifact the agent touches.

### Bind mount: `./shared-workspace`
Use for:
- explicit handoff files
- scratch artifacts you want visible from both the host and the agent workspace
- shared outputs that should not be mixed into the curated startup overlays or durable submounts

This exists so the startup workspace can stay small and role-defining while still giving the agent a visible place for shared outputs.

---

## Practical rule

- If humans edit it often, make it an explicit bind mount.
- If it is one of the common durable `.openclaw` paths (`workspace`, `media`, `agents`, `memory`, `tmp`), consider an explicit submount instead of a full-tree bind mount.
- If OpenClaw owns it operationally and humans rarely need it, keep it in the named volume.
- If it is meant for handoff, keep it in `shared-workspace` instead of smearing it into random runtime paths.
- If it is the broader project tree, mount it separately from the startup workspace so agents can distinguish identity files from the repo they are working in.
