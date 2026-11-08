# Changelog

## [0.1.0] - Unreleased
### Distribution
- Go module path is now `github.com/shamim3696/cache-boy`, so `go install github.com/shamim3696/cache-boy/cmd/...@latest` works
- Release pipeline: pushing a `v*` tag builds Linux/macOS/Windows (amd64 + arm64) archives with GoReleaser, a multi-arch image on ghcr.io and publishes `cache-boy-client` to npm (see docs/releasing.md)
- `scripts/install.sh` and `scripts/install.ps1` one-line installers with SHA-256 verification
- `cache-boy --version` and `--help`
- CI on Linux (race detector), Windows and macOS, plus SDK tests and a Docker smoke test
- MIT LICENSE

### Added
- Go project folder structure
