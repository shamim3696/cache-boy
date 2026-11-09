# Installs cache-boy, cache-boy-cli, cache-boy-bench and cache-boy-sync on Windows.
#
#   irm https://raw.githubusercontent.com/shamim3696/cache-boy/main/scripts/install.ps1 | iex
#
# Environment:
#   CACHE_BOY_VERSION      release tag such as v0.1.0 (default: latest)
#   CACHE_BOY_INSTALL_DIR  where binaries go (default: %LOCALAPPDATA%\cache-boy\bin)
#   CACHE_BOY_BASE_URL     download from a mirror instead of GitHub releases
#   CACHE_BOY_NO_MODIFY_PATH  set to anything to leave your user PATH alone
#
# Works in Windows PowerShell 5.1 and PowerShell 7. No admin rights needed:
# binaries go under your profile and only your user PATH is changed.

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue' # Invoke-WebRequest is very slow with the progress bar

& {
    $repo = 'shamim3696/cache-boy'
    $version = if ($env:CACHE_BOY_VERSION) { $env:CACHE_BOY_VERSION } else { 'latest' }

    $arch = switch ($env:PROCESSOR_ARCHITECTURE) {
        'AMD64' { 'amd64' }
        'ARM64' { 'arm64' }
        default { throw "cache-boy: unsupported CPU $($env:PROCESSOR_ARCHITECTURE) (amd64 and arm64 are built)" }
    }

    if ($env:CACHE_BOY_BASE_URL) {
        $base = $env:CACHE_BOY_BASE_URL.TrimEnd('/')
    } elseif ($version -eq 'latest') {
        $base = "https://github.com/$repo/releases/latest/download"
    } else {
        $base = "https://github.com/$repo/releases/download/$version"
    }

    $dir = if ($env:CACHE_BOY_INSTALL_DIR) { $env:CACHE_BOY_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'cache-boy\bin' }

    # Windows PowerShell 5.1 may default to TLS 1.0, which GitHub refuses.
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

    $archive = "cache-boy_windows_$arch.zip"
    $tmp = Join-Path ([IO.Path]::GetTempPath()) ("cache-boy-" + [guid]::NewGuid())
    New-Item -ItemType Directory -Path $tmp | Out-Null
    try {
        Write-Host "Downloading $archive ($version)..."
        Invoke-WebRequest -UseBasicParsing -Uri "$base/$archive" -OutFile (Join-Path $tmp $archive)
        Invoke-WebRequest -UseBasicParsing -Uri "$base/checksums.txt" -OutFile (Join-Path $tmp 'checksums.txt')

        $line = Get-Content (Join-Path $tmp 'checksums.txt') | Where-Object { $_ -match " $([regex]::Escape($archive))$" } | Select-Object -First 1
        if (-not $line) { throw "cache-boy: $archive is not listed in checksums.txt" }
        $want = ($line -split '\s+')[0]
        $got = (Get-FileHash -Algorithm SHA256 (Join-Path $tmp $archive)).Hash
        if ($got -ne $want) { throw "cache-boy: checksum mismatch for $archive" }

        Expand-Archive -Path (Join-Path $tmp $archive) -DestinationPath (Join-Path $tmp 'x') -Force
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        foreach ($b in 'cache-boy', 'cache-boy-cli', 'cache-boy-bench', 'cache-boy-sync') {
            $src = Join-Path $tmp "x\$b.exe"
            if (Test-Path $src) { Copy-Item -Force $src (Join-Path $dir "$b.exe") } # older releases lack some tools
        }
        foreach ($c in 'cache-boy.conf', 'cache-boy-sync.yaml') {
            $conf = Join-Path $tmp "x\configs\$c"
            if (Test-Path $conf) { Copy-Item -Force $conf (Join-Path $dir $c) }
        }
    } finally {
        Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
    }

    Write-Host "Installed to $dir"

    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $parts = @(if ($userPath) { $userPath -split ';' | Where-Object { $_ } })
    if ($env:CACHE_BOY_NO_MODIFY_PATH) {
        Write-Host "Left PATH unchanged; add $dir to it yourself."
    } elseif ($parts -notcontains $dir) {
        [Environment]::SetEnvironmentVariable('Path', (($parts + $dir) -join ';'), 'User')
        Write-Host "Added $dir to your user PATH (open a new terminal to pick it up)."
    }
    if (($env:Path -split ';') -notcontains $dir) { $env:Path = "$env:Path;$dir" }

    Write-Host ''
    Write-Host 'Start the server:  cache-boy'
    Write-Host 'Open a shell:      cache-boy-cli'
    Write-Host 'Dashboard:         http://127.0.0.1:6381'
}
