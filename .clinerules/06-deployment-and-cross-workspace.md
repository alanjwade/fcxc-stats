# Deployment, Versioning & cross-workspace relationship

Two repos (or more, for future apps) work together. Keep the split clean:

- **`fcxc_stats` (this repo)** — app source of truth. All code lives here.
- **`homelab-infra`** (sibling workspace) — the only place that knows anything
  about where/what the app runs on.

## The split

- **This repo's only job is to ship a container image.** On a `v*` release tag,
  `.github/workflows/build-push.yml` builds and publishes
  `ghcr.io/alanjwade/fcxc-stats:<tag>`. That's it — it does not touch, notify,
  or know anything about a host, a compose file, a deploy path, or a machine.
- **homelab-infra owns all deployment.** The pinned image tag, the compose
  file, the host, and the up-to-date check live in
  `homelab-infra/hosts/homelab01/fcxc-stats/`. A general-purpose script there
  (`homelab-infra/scripts/update-apps.sh`) watches GHCR, bumps the pinned tag,
  commits/pushes, then sshs into the host to `git pull` + `docker compose pull`
  + `docker compose up -d`. homelab-infra discovers this app because that
  service folder carries an `app.yml` manifest.

## Release flow

1. **Create & push the release tag (local).** `bump_release.sh` is the single
   command to ship a release. On a clean tree it reads the newest `vX.Y.Z` tag,
   computes the next version (patch by default; `minor`/`major` accepted),
   pushes the branch HEAD, and creates/pushes the new tag:
   ```bash
   ./bump_release.sh          # v1.0.7 -> v1.0.8
   ./bump_release.sh minor    # v1.0.7 -> v1.1.0
   ```
   It is **target-agnostic** — it makes no file edits and knows nothing about the
   deployment host. The version is not stored in any committed file.

2. **CI publishes the image only (GitHub):** `build-push.yml` builds and pushes
   `ghcr.io/alanjwade/fcxc-stats:<tag>`, passing `APP_VERSION=<tag>` as a build
   arg. Nothing else happens — no notification to homelab-infra and no
   infrastructure knowledge in this repo.

3. **The tag appears in the webapp footer.** The Dockerfile stores the build-time
   `APP_VERSION` as a runtime env var; Flask injects it (`app.context_processor`)
   and `base.html` renders it. Locally (no Docker) it falls back to `dev`.
   The footer therefore always reflects the version of the running image.

4. **homelab-infra deploys it (on the host / from the infra checkout):**
   ```bash
   cd homelab-infra && ./scripts/update-apps.sh
   # or just one app:  ./scripts/update-apps.sh fcxc-stats
   ```

## Environment rule

- Never hardcode machine-specific secrets or the production `SECRET_KEY` in
  committed files. Config comes from `.env` (`cp .env.example .env`, fill real
  values). The committed `docker-compose.yml` (in homelab-infra) reads env vars
  (`VIRTUAL_HOST`, `TZ`, `DATABASE_URL`) and never commits `.env`.

## Data / backups

- The SQLite database lives on the server at `/opt/homelab/fcxc-stats/data/`,
  declared in homelab-infra's `hosts/homelab01/fcxc-stats/backup.yml`
  (`local: true`, `offsite: false`, no `pre_backup` — the raw SQLite file is
  backed up directly). This repo does not need to concern itself with it; the
  backup builder lives in homelab-infra.
