# Build Kova-x64-setup.exe with Inno Setup 6.
# BINS is the directory that holds kova.exe, kova-setup.exe, kovadump.exe, and iggy-server.exe.
# OUT is the directory for the installer. The default is dist\ next to the repository.

$ErrorActionPreference = 'Stop'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent (Split-Path -Parent $here)
$bins = if ($env:BINS) { $env:BINS } else { Join-Path $root 'target\release' }
$out = if ($env:OUT) { $env:OUT } else { Join-Path $root 'dist' }

foreach ($name in @('kova.exe', 'kova-setup.exe', 'kovadump.exe', 'iggy-server.exe')) {
    $path = Join-Path $bins $name
    if (-not (Test-Path -LiteralPath $path)) {
        Write-Error "missing $path"
    }
}

$iscc = @(
    "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    "$env:ProgramFiles\Inno Setup 6\ISCC.exe"
) | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1

if (-not $iscc) {
    Write-Error 'Inno Setup 6 is not installed'
}

New-Item -ItemType Directory -Force -Path $out | Out-Null
& $iscc "/DBinDir=$bins" "/O$out" "/FKova-x64-setup" (Join-Path $here 'kova.iss')
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$setup = Join-Path $out 'Kova-x64-setup.exe'
$hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $setup).Hash.ToLowerInvariant()
Set-Content -LiteralPath "$setup.sha256" -Value "$hash  Kova-x64-setup.exe" -Encoding ascii
Write-Output $setup
