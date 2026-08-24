# Cline Rules — fcxc_stats

This directory is loaded by Cline for every task in this workspace. Read it
before modifying anything. Rules are named with numeric prefixes so they load
in a sensible order:

| File                        | When it matters                          |
| --------------------------- | ---------------------------------------- |
| `01-project-overview.md`    | Always — what this app is and its shape |
| `02-repository-structure.md`| Finding/adding files, sources of truth  |
| `03-code-conventions.md`    | Writing Python/Flask/SQL/parsers        |
| `04-scraper-workflow.md`    | Adding a race, running/editing the scraper |
| `05-tests-and-dev.md`       | Running the webapp locally, running tests |
| `06-deployment-and-cross-workspace.md` | Versioning, GHCR, and the homelab-infra relationship |