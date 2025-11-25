# Exasol Docker Container

## ARM

Hint: Exasol requires privileged mode.

```bash
docker run --name exasoldb \
            -p 8563:8563 \
            -d \
            --privileged \
            exasol/docker-db:2025.2.0-arm64dev.0
```