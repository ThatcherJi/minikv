param(
    [int]$CoordPort = 7000
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$DemoDir = Join-Path $Root "demo-data"
$Processes = @()

function Start-MinikvProcess {
    param(
        [string]$Name,
        [string[]]$Arguments
    )

    $log = Join-Path $DemoDir "$Name.log"
    $process = Start-Process `
        -FilePath "cargo" `
        -ArgumentList $Arguments `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $log `
        -RedirectStandardError $log `
        -PassThru `
        -WindowStyle Hidden
    $script:Processes += $process
    return $process
}

function Stop-DemoProcesses {
    foreach ($process in $script:Processes) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

try {
    if (Test-Path $DemoDir) {
        Remove-Item -LiteralPath $DemoDir -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $DemoDir | Out-Null

    cargo build

    Start-MinikvProcess "coord" @(
        "run", "--bin", "coord", "--",
        "--listen", "127.0.0.1:$CoordPort",
        "--replicas", "2",
        "--vnodes", "64",
        "--dead-after-secs", "4",
        "--meta", (Join-Path $DemoDir "coord-meta.json")
    ) | Out-Null

    Start-Sleep -Seconds 1

    foreach ($i in 1..3) {
        $port = $CoordPort + $i
        Start-MinikvProcess "volume-v$i" @(
            "run", "--bin", "volume", "--",
            "--id", "v$i",
            "--listen", "127.0.0.1:$port",
            "--coord", "http://127.0.0.1:$CoordPort",
            "--data", (Join-Path $DemoDir "v$i"),
            "--heartbeat-secs", "1"
        ) | Out-Null
    }

    Start-Sleep -Seconds 3

    Write-Host "== cluster after registration =="
    cargo run --bin cli -- --coord "http://127.0.0.1:$CoordPort" cluster

    Write-Host "== put/get before failure =="
    cargo run --bin cli -- --coord "http://127.0.0.1:$CoordPort" put k1 v1
    cargo run --bin cli -- --coord "http://127.0.0.1:$CoordPort" get k1

    Write-Host "== ring placement for k1 =="
    cargo run --bin cli -- --coord "http://127.0.0.1:$CoordPort" ring k1

    Write-Host "== volume stats via coordinator =="
    cargo run --bin cli -- --coord "http://127.0.0.1:$CoordPort" volume-stats

    Write-Host "== direct volume keys and compaction =="
    cargo run --bin cli -- keys "127.0.0.1:$($CoordPort + 1)"
    cargo run --bin cli -- compact "127.0.0.1:$($CoordPort + 1)"

    Write-Host "== kill one volume =="
    Stop-Process -Id $Processes[1].Id -Force
    Start-Sleep -Seconds 6

    Write-Host "== cluster after killing one volume =="
    cargo run --bin cli -- --coord "http://127.0.0.1:$CoordPort" cluster

    Write-Host "== read after killing one volume =="
    cargo run --bin cli -- --coord "http://127.0.0.1:$CoordPort" get k1
}
finally {
    Stop-DemoProcesses
}
