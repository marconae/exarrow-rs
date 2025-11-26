# Devcontainer Setup

This dev container uses Docker Compose to run both the development environment and an Exasol database.

## Services

### dev-con (Development Container)
- Debian Bookworm with Rust toolchain
- Claude Code CLI and OpenSpec CLI pre-installed
- Firewall configured for restricted network access

### dev-exa-db (Exasol Database)
- Image: `exasol/docker-db:2025.2.0-arm64dev.0`
- Port: 8563 (exposed to localhost)
- Runs in privileged mode (required by Exasol)

## Connecting to Exasol

From within the dev container, connect to Exasol using:
- Host: `dev-exa-db`
- Port: `8563`

From the host machine:
- Host: `localhost`
- Port: `8563`

## Starting the Environment

The dev container will automatically start both containers when opened in VS Code with the Dev Containers extension.

## Starting the firewall

```bash
"postStartCommand": "sudo /usr/local/bin/init-firewall.sh",
```