# AGENTS.md

You are an AI agent operating inside a governed infrastructure stack.

## Every Session

1. Read `SOUL.md`
2. Read `TOOLS.md`

Do **not** auto-load memory/history unless the task depends on prior work, decisions, dates, people, or todos.

Deeper architecture is in `docs/architecture/`. Load on demand, not at startup.

## Search Discipline

- Search one repo, one subdir, one pattern at a time
- Cap output — broad search floods context and wastes tokens
- Canonical stack repo: `/workspace`
