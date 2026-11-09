# Changelog

## [0.2.0] - 2026-11-09
### Added
- Automatic cache invalidation from databases (docs/db-sync.md). cache-boy reads the change stream of PostgreSQL (logical replication), MySQL/MariaDB (binlog) and MongoDB (change streams) and, per configured rule, invalidates tags and deletes keys built from each changed row (`user:{id}`). Old and new rows both count on updates; delivery is at least once (replication slot or state file), so changes made while it or cache-boy is down are not lost.
- It is opt-in: nothing happens unless the config says `auto_invalidate: true`; otherwise the application invalidates the cache itself.
- Run it inside the server with `db-sync <file>` in cache-boy.conf, or as the separate `cache-boy-sync` command (`-dry-run`, `-uninstall`, `-v`).
- Install scripts and release archives include `cache-boy-sync` and an example `configs/cache-boy-sync.yaml`.

## [0.1.0] - 2026-11-08
### Distribution
- Go module path is now `github.com/shamim3696/cache-boy`, so `go install github.com/shamim3696/cache-boy/cmd/...@latest` works
- Release pipeline: pushing a `v*` tag builds Linux/macOS/Windows (amd64 + arm64) archives with GoReleaser, a multi-arch image on ghcr.io and publishes `cache-boy-client` to npm (see docs/releasing.md)
- `scripts/install.sh` and `scripts/install.ps1` one-line installers with SHA-256 verification
- `cache-boy --version` and `--help`
- CI on Linux (race detector), Windows and macOS, plus SDK tests and a Docker smoke test
- MIT LICENSE

### Added
- Go project folder structure
