# Adding a Service to homelab-infra

This document explains how to integrate an external project into this
repository so it runs alongside the existing homelab stack.

## Repository layout

```
homelab-infra/
└── hosts/
    └── <hostname>/          ← one folder per physical host
        └── <service>/       ← one folder per service on that host
            ├── docker-compose.yml
            ├── .env.example
            ├── .env          ← secrets, never committed
            └── backup.yml
```

Pick the correct host folder (`homelab01`, etc.) and create a subfolder
named after your service (lowercase, hyphen-separated, e.g.
`my-cool-app`).

## Required files

### `docker-compose.yml`

Declarative service definition. Rules:

- Use a **pinned image tag** — never `:latest`.
  `ghcr.io/you/app:v1.2` is fine; `ghcr.io/you/app:latest` is not.
  The compose file should be sufficient to know exactly what is deployed.
- Set `restart: unless-stopped` on every service.
- Mount persistent data to `/opt/homelab/<service>/` on the host
  (see [Data directories](#data-directories) below), not to a
  repo-relative path.
- Read all secrets from environment variables sourced from `.env`.

Minimal example:

```yaml
services:
  my-cool-app:
    container_name: my-cool-app
    image: ghcr.io/you/my-cool-app:v1.2.3
    restart: unless-stopped
    environment:
      - SECRET_KEY=${SECRET_KEY}
      - TZ=${TZ}
    volumes:
      - /opt/homelab/my-cool-app/data:/app/data
    ports:
      - "8080:8080"
```

### `.env.example`

Documents every variable the compose file uses, with placeholder values.
This file **is** committed. The real `.env` is gitignored.

```dotenv
# Copy to .env and fill in real values
SECRET_KEY=change-me
TZ=America/New_York
```

### `backup.yml`

Declares what needs to be backed up. Copy from `backup.yml.example` if
one exists in the same service folder, or use the template below.

```yaml
# See /README.md#backup-system for full documentation.

pre_backup: []   # shell commands to run before paths are snapshotted
                 # (e.g. a pg_dump, an app-level export)
                 # Leave [] if raw files are already safe to copy.

paths:
  - /opt/homelab/my-cool-app/data

tiers:
  local: true      # back up to local restic repo (USB drive)
  offsite: false   # back up to rclone → S3 Glacier Deep Archive
```

Set `local: false` and `offsite: false` and leave `paths: []` for
stateless or purely-config services that have nothing worth snapshotting.

## Data directories

Create the host-side data directories **before** first deploy:

```bash
sudo mkdir -p /opt/homelab
sudo chown $USER:$USER /opt/homelab

mkdir -p /opt/homelab/my-cool-app/data
```

The compose file expects these paths to exist. The directories are
intentionally outside the git repo so runtime data never enters version
control.

## Deploying

```bash
cd hosts/<hostname>/my-cool-app
cp .env.example .env   # fill in real values
docker compose up -d
docker compose logs -f
```

## Checklist

- [ ] Folder created at `hosts/<hostname>/my-cool-app/`
- [ ] `docker-compose.yml` uses a pinned tag and `restart: unless-stopped`
- [ ] All secrets are environment variables; no hardcoded credentials
- [ ] Volumes point to `/opt/homelab/my-cool-app/...`, not repo-relative paths
- [ ] `.env.example` documents every variable
- [ ] `.env` is gitignored (already handled by root `.gitignore`)
- [ ] `backup.yml` declares paths and tiers
- [ ] Host-side data directories created before first deploy
- [ ] `docker compose up -d` succeeds and logs look healthy
