using module .\TaskPool
Import-Module .\powershell-yaml


$workDir = Get-Location

$conf = Get-Content "config.yml" | ConvertFrom-Yaml
$targets = Get-Content $conf.対象ホスト一覧 | ConvertFrom-Csv | foreach { "$($_.アドレス)" }

$conf.対象ホスト = $targets

$conf.ログ保存先 = (Resolve-Path $conf.ログ保存先).Path
$conf.結果保存先 = (Resolve-Path $conf.結果保存先).Path

if ($conf.配布ファイル.Count -eq 0) {
    $conf.配布ファイル = @()
}
$conf.配布ファイル = $conf.配布ファイル | foreach {
    $_.ファイル = $_.ファイル | foreach { (Resolve-Path $_ -ErrorAction Stop).Path }
    $_
}

if ($conf.回収ファイル.Count -eq 0) {
    $conf.回収ファイル = @()
}

if ($conf.並列実行数 -lt 1) {
    Write-Warning "並列実行数は1未満に出来ません。1として実行します。"
    $conf.並列実行数 = 1
}
if ($conf.最大試行回数 -lt 1) {
    Write-Warning "最大試行回数は1未満に出来ません。1として実行します。"
    $conf.最大試行回数 = 1
}

$conf | ConvertTo-Json | Out-Host

$credential = Get-Credential $conf.ユーザ名 -ErrorAction Stop
if (-not $credential) {
    exit
}

$pool = [TaskPool]::new($conf.並列実行数)

foreach ($t in $targets) {
    $pool.Add([Task]@{
        Name = "$($conf.タスク名)_${t}"
        MaxRetry = $conf.最大試行回数 - 1
        Arguments = @($t, $conf, $workDir)
        Action = {
            param([string]$address, [Hashtable]$conf, [string]$workDir)

            ping -n 1 $address | Out-Null
            if ($LastExitCode -ne 0) {
                throw "ホストに接続できませんでした"
            }

            Set-Location $workDir

            foreach ($fileSet in $conf.配布ファイル) {
                $path = "\\${address}\$($fileSet.配布先 -replace "(^[a-zA-Z]):","`$1$")"

                New-PSDrive FileDstributor -PSProvider FileSystem -Root $path -Credential $credential -ErrorAction Stop

                if (-not (Test-Path -Type Container "FileDstributor:/")) {
                    throw "対象フォルダに接続できませんでした"
                }

                foreach ($file in $fileSet.ファイル) {
                    Copy-Item $file "FileDstributor:/"
                }

                Remove-PSDrive FileDstributor
            }

            foreach ($file in $conf.回収ファイル) {
                $path = "\\${address}\$((Split-Path $file.ファイル) -replace "(^[a-zA-Z]):","`$1$")"
                New-PSDrive FileDstributor -PSProvider FileSystem -Root $path -Credential $credential -ErrorAction Stop

                if (-not (Test-Path -Type Container "FileDstributor:/")) {
                    throw "対象フォルダに接続できませんでした"
                }

                $fname = "FileDstributor:/$(Split-Path -Leaf $file.ファイル)"
                if (-not (Test-Path $fname)) {
                    throw "回収ファイル `"${file}`" が見つかりません"
                }

                $targetDir = $file.回収先 -replace "HOST_ADDRESS",$address
                if (-not (Test-Path $targetDir)) {
                    mkdir $targetDir
                }

                if (Test-Path -Type Leaf $fname) {
                    Copy-Item $fname $targetDir
                } else {
                    $target = "${targetDir}/$(Split-Path -Leaf $fname).csv"
                    Get-ChildItem $fname | select -Property Name,Length,Mode,CreationTime,LastWriteTime,LastAccessTime | Export-Csv $target -NoTypeInformation -Encoding Default
                }

                Remove-PSDrive FileDstributor
            }
        }
    })
}

$status = [PSCustomObject]@{
    Executed = 0
    Completed = 0
    Errored = 0
    SuccessHosts = @{}
    TryCount= @{}
}

$writeStatus = {
    Write-Progress "$($conf.タスク名)" "$($targets.Count)ホスト中$($status.Completed)ホスト完了 （$($status.Executed)回実行 : $($status.Completed)回成功 : $($status.Errored)回失敗）" -Id 0 -PercentComplete ($status.Completed * 100 / $targets.Count)
}.GetNewClosure()

$writeLog = {
    param($hostAddress, $errorReason)

    [PSCustomObject]@{
        日時 = Get-Date
        タスク名 = "$($conf.タスク名)"
        ターゲットホスト = "$($hostAddress)"
        結果 = if ($errorReason -eq $null) { "成功" } else { "失敗" }
        理由 = $errorReason
        成功数 = $status.Completed
        失敗数 = $status.Errored
        実行数 = $status.Executed
        対象ホスト数 = $targets.Count
    } | Export-Csv $conf.ログ保存先 -NoTypeInformation -Append -Encoding Default
}.GetNewClosure()

$pool.OnTaskComplete.Add({
    $target = $_.Task.Arguments[0]

    $status.Executed += 1
    $status.Completed += 1
    $status.SuccessHosts[$target] = 1
    $status.TryCount[$target] += 1

    & $writeStatus
    & $writeLog $target $null
}.GetNewClosure())

$pool.OnTaskError.Add({
    $target = $_.Task.Arguments[0]

    $status.Executed += 1
    $status.Errored += 1
    $status.TryCount[$target] += 1

    & $writeStatus
    & $writeLog $target $_.Error

    Write-Warning "${target}: $($_.Error)"
}.GetNewClosure())

& $writeStatus $status
try {
    $pool.Run()
} finally {
    $targets | foreach {
        [PSCustomObject]@{
            ターゲットホスト = $_
            結果 = if ($status.SuccessHosts.ContainsKey($_)) { "成功" } else { "失敗" }
            試行回数 = $status.TryCount[$_]
        }
    } | Export-Csv $conf.結果保存先 -NoTypeInformation -Encoding Default
}
