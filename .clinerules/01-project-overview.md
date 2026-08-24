# fcxc_stats — Project Overview

Fort Collins High School cross-country statistics tracker.

- **Web dashboard** (Flask + SQLAlchemy) that reads a SQLite database and
  serves athlete/team statistics. Runs in Docker and is deployed on a remote
  homelab host via the `homelab-infra` repo.
- **Standalone scraper** that pulls race results from MileSplit, parses them,
  and populates the *same* SQLite database. Runs locally (outside Docker) in a
  Python virtual environment.

```
┌─────────────┐     ┌──────────┐    ┌─────────────┐
│  Scraper    │───▶ │  SQLite  │◀──▶│   Webapp    │
│ (local venv)│     │ database │    │  (Docker)   │
└─────────────┘     └──────────┘    └─────────────┘
```

The scraper does not run in the deployed container; it only runs on your dev
machine to gather data. The webapp only *reads* the resulting database.

## Two working areas, one workspace

- **App source lives here** in `fcxc_stats` — this workspace is the
  source-of-truth codebase. All code changes (webapp, scraper, parsers,
  schema) belong here.
- **Deployment wiring lives in the sibling `homelab-infra` workspace**
  (`hosts/homelab01/fcxc-stats/`). Do not edit the deployed compose config
  here as a substitute for that repo; see `06-deployment-and-cross-workspace.md`.

## Git / versioning

- `main` is the working branch. Production releases are **git tags** named
  `v*` (e.g. `v1.0.6`).
- Pushing a `v*` tag triggers the GitHub Actions workflow
  (`.github/workflows/build-push.yml`) to build & push
  `ghcr.io/alanjwade/fcxc-stats:<tag>`, then dispatch a `fcxc-stats-release`
  event to `homelab-infra` to bump the deployed compose tag.
- Real secrets (`.env`) and local scratch test files (`test_*.py`) are
  gitignored; never commit them.