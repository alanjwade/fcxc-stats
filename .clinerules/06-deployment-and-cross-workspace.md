# Deployment, Versioning & cross-workspace relationship

Two repos work together. Keep the split clear:

- **`fcxc_stats` (this repo)** — app source of truth. All code lives here.
- **`homelab-infra`** (sibling workspace) — deployment wiring. The deployed
  compose config lives at `homelab-infra/hosts/homelab01/fcxc-stats/`.
  Code changes go here in `fcxc_stats`; **compose/version/deploy changes go to
  homelab-infra**. Don't edit the app code as if it lived in the infra repo, and
  don't change deployment wiring in this repo as a substitute for the infra repo.

## `homelab-deployment/` — the bridge

This folder holds the **three files** that are mirrored into the infra repo's
`hosts/homelab01/fcxc-stats/`:

- `homelab-deployment/docker-compose.yml`  →  `hosts/homelab01/fcxc-stats/docker-compose.yml`
- `homelab-deployment/.env.example`        →  `hosts/homelab01/fcxc-stats/.env.example`
- `homelab-deployment/backup.yml`          →  `hosts/homelab01/fcxc-stats/backup.yml`

Keep these in sync if you change one side. The `docker-compose.yml` pins
`image: ghcr.io/alanjwade/fcxc-stats:<tag>` and mounts
`/opt/homelab/fcxc-stats/data:/data`.

## Release flow (bump + tag → GHCR → infra auto-update → deploy)

1. **Bump & tag (local):**
   ```bash
   ./bump_version_homelab01.sh             # patch bump (v1.0.0 → v1.0.1)
   ./bump_version_homelab01.sh minor       # v1.0.0 → v1.1.0
   ./bump_version_homelab01.sh major       # v1.0.0 → v2.0.0
   ```
   It requires a clean working tree, updates the tag in
   `homelab-deployment/docker-compose.yml`, commits, pushes, and creates/pushes
   a `v*` git tag.

2. **CI builds (GitHub):** `.github/workflows/build-push.yml` triggers on
   `v*` tags, builds `ghcr.io/alanjwade/fcxc-stats:<tag>`, pushes it, and
   dispatches a `fcxc-stats-release` event to `homelab-infra` (needs the
   `HOMELAB_INFRA_REPO_TOKEN` secret). A workflow in homelab-infra then
   re-writes the deployed compose image tag and commits.

3. **Deploy (remote):**
   ```bash
   ./deploy_homelab01.sh
   ```
   Verifies the image exists in GHCR, SSHes to homelab01, and restarts the
   container with the new image.

## Environment rule

- Never hardcode machine-specific secrets or the production `SECRET_KEY` in
  committed files. Config comes from `.env` (`cp .env.example .env`, fill real
  values). The committed `docker-compose.yml` reads env vars
  (`VIRTUAL_HOST`, `TZ`, `DATABASE_URL`) and never commits `.env`.

## Data / backups

- The SQLite database lives on the server at `/opt/homelab/fcxc-stats/data/`,
  declared in `homelab-deployment/backup.yml` (`local: true`, `offsite: false`,
  no `pre_backup` — raw SQLite file is backed up directly).
- To seed the server DB from local data: `scp data/fcxc_stats.db
  homelab@homelab01:/opt/homelab/fcxc-stats/data/`.