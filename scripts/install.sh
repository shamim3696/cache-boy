#!/bin/sh
# Installs cache-boy, cache-boy-cli, cache-boy-bench and cache-boy-sync on Linux or macOS.
#
#   curl -fsSL https://raw.githubusercontent.com/shamim3696/cache-boy/main/scripts/install.sh | sh
#
# Environment:
#   CACHE_BOY_VERSION      release tag such as v0.1.0 (default: latest)
#   CACHE_BOY_INSTALL_DIR  where binaries go (default: /usr/local/bin if
#                          writable, else ~/.local/bin)
#   CACHE_BOY_BASE_URL     download from a mirror instead of GitHub releases
#
# It never edits shell profiles; if the install dir isn't on PATH it says so.
set -eu

repo="shamim3696/cache-boy"
version="${CACHE_BOY_VERSION:-latest}"

fail() { echo "install.sh: $*" >&2; exit 1; }

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) fail "unsupported OS $(uname -s); on Windows use scripts/install.ps1" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) fail "unsupported CPU $(uname -m) (amd64 and arm64 are built)" ;;
esac

if [ -n "${CACHE_BOY_BASE_URL:-}" ]; then
  base="$CACHE_BOY_BASE_URL"
elif [ "$version" = latest ]; then
  base="https://github.com/$repo/releases/latest/download"
else
  base="https://github.com/$repo/releases/download/$version"
fi

if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO "$2" "$1"; }
else
  fail "curl or wget is required"
fi

if [ -n "${CACHE_BOY_INSTALL_DIR:-}" ]; then
  dir="$CACHE_BOY_INSTALL_DIR"
elif [ -w /usr/local/bin ]; then
  dir=/usr/local/bin
else
  dir="$HOME/.local/bin"
fi

archive="cache-boy_${os}_${arch}.tar.gz"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

echo "Downloading $archive ($version)..."
fetch "$base/$archive" "$tmp/$archive" || fail "download failed: $base/$archive"
fetch "$base/checksums.txt" "$tmp/checksums.txt" || fail "download failed: $base/checksums.txt"

want="$(grep " $archive\$" "$tmp/checksums.txt" | cut -d' ' -f1)"
[ -n "$want" ] || fail "$archive is not listed in checksums.txt"
if command -v sha256sum >/dev/null 2>&1; then
  got="$(sha256sum "$tmp/$archive" | cut -d' ' -f1)"
else
  got="$(shasum -a 256 "$tmp/$archive" | cut -d' ' -f1)"
fi
[ "$got" = "$want" ] || fail "checksum mismatch for $archive"

tar -xzf "$tmp/$archive" -C "$tmp"
mkdir -p "$dir"
for b in cache-boy cache-boy-cli cache-boy-bench cache-boy-sync; do
  [ -f "$tmp/$b" ] || continue # older releases lack some tools
  install -m 0755 "$tmp/$b" "$dir/$b"
done

echo "Installed to $dir:"
"$dir/cache-boy" --version 2>/dev/null || true
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "Add $dir to your PATH:  export PATH=\"$dir:\$PATH\"" ;;
esac
echo
echo "Start the server:  cache-boy"
echo "Open a shell:      cache-boy-cli"
echo "Dashboard:         http://127.0.0.1:6381"
