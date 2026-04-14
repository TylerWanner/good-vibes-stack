# safe-docker Runtime Notes

This is the practical contract for running `safe-docker` inside the Good Vibes Stack.

It is not a full safe-docker spec. It is the set of constraints that matter for this repo.

---

## Default posture

The default policy keeps safe-docker narrow:

- read-only inspection: `status`, `logs`
- routine lifecycle: `restart`
- dangerous writes: `build` only where explicitly allowed and approval-gated
- `recreate` is **not enabled by default**

Why:
- `restart` is the normal operational tool
- `build` is the explicit human-in-the-loop deployment gate for baked images
- `recreate` is useful, but sharper, easier to misuse, and more sensitive to compose path issues

---

## `restart` vs `recreate`

These are not interchangeable.

### `restart`
Use when:
- container config has not changed
- you want to bounce a service cleanly
- you do not need Compose to re-evaluate service definition details

This should be the default operational move.

### `recreate`
Use when:
- the service definition changed in a way that requires container replacement
- you intentionally want Compose to replace the container

This is a more dangerous tool and should stay opt-in in policy.

---

## Containerized compose writes need path parity

If safe-docker runs **inside a container** and performs compose-backed writes like `build` or `recreate`, the managed project root should be mounted into the safe-docker container at the **same absolute path** the host uses.

Good pattern:
```yaml
volumes:
  - ${PWD}:${PWD}
```

Bad pattern:
```yaml
volumes:
  - ./:/project
```

Why this matters:
- Compose mutations resolve bind mounts against host reality
- a synthetic in-container alias can work for light inspection and still break writes
- the failure mode is confusing: reads can appear healthy while write operations behave strangely

The repo compose file therefore mounts `${PWD}` into safe-docker at `${PWD}`.

---

## Policy guidance

For Good Vibes Stack, the intended default is:

- allow `restart` on routine services
- allow `build` only where a baked-image rebuild is part of the intended workflow
- require HITL approval for dangerous operations
- leave `recreate` disabled until there is a clear, tested need for it

This keeps the operational contract simple:
- change code
- human approves build
- rebuild the baked service image
- restart or bring the service back through the normal path

---

## Backend split: inspection vs mutation

The current practical split is:

- **inspection/read-ish paths** can use SDK-backed or label-backed lookups
- **mutation paths** should prefer the Compose CLI when Compose semantics matter
- **`recreate` in particular** should be treated as a Compose CLI operation, not as a fancier container restart

This is not abstract purity. It comes from the actual failure mode where SDK-backed recreate behaved differently from CLI-backed recreate.

---

## What this doc is trying to prevent

Three common mistakes:

1. Treating `recreate` like a fancier `restart`
2. Enabling dangerous compose writes before there is a real use case
3. Running safe-docker in a container with a fake project path and assuming compose writes will behave the same as reads

The theme is the same in all three cases: the control plane should stay boring.
