# Releasing

A release is one tag push. `.github/workflows/release.yml` then runs the Go tests and, if they pass, does three things in parallel:

| Job | Publishes |
|---|---|
| `binaries` | A GitHub release with `cache-boy_{linux,darwin,windows}_{amd64,arm64}` archives and `checksums.txt` (built by GoReleaser, `.goreleaser.yaml`) |
| `docker` | `ghcr.io/shamim3696/cache-boy:<version>`, `:<major>.<minor>` and `:latest` for linux/amd64 and linux/arm64 |
| `npm` | `cache-boy-client@<version>`, with the version taken from the tag |

Tags with a hyphen (`v0.2.0-rc.1`) are prereleases: Docker skips `:latest` and npm publishes under the `next` dist-tag.

## One-time setup

1. **npm token.** On npmjs.com, create a *granular access token* with publish rights. Add it to the GitHub repository as the secret `NPM_TOKEN` (Settings → Secrets and variables → Actions).
2. **Make the Docker image public.** After the first release, open the `cache-boy` package on your GitHub profile → Package settings → Change visibility → Public. GHCR packages start private.
3. **Workflow permissions.** Settings → Actions → General → Workflow permissions → "Read and write permissions", so the `binaries` job can create the release.

## Each release

```bash
# 1. Move the Unreleased notes in CHANGELOG.md under the new version and commit.
# 2. Tag and push:
git tag v0.1.0
git push origin main v0.1.0
```

Watch it under the repository's Actions tab. When it finishes:

- `irm .../scripts/install.ps1 | iex` and `curl .../scripts/install.sh | sh` install the new version (they always fetch `releases/latest`)
- `docker run ghcr.io/shamim3696/cache-boy` runs it
- `npm install cache-boy-client` gets the SDK

## Trying a release locally

```bash
# Build every archive into dist/ without publishing anything
docker run --rm -v "$PWD:/src" -w /src -e GOTOOLCHAIN=auto goreleaser/goreleaser:v2.12.0 release --snapshot --clean

# Install from that folder instead of GitHub (serve dist/ on any static HTTP server first)
CACHE_BOY_BASE_URL=http://127.0.0.1:8765 sh scripts/install.sh
```

The version printed by `cache-boy --version` comes from the tag through `-ldflags -X .../internal/command.Version`; a plain `go build` reports `0.1.0-dev`.
