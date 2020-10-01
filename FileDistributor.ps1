Import-Module .\TaskPool
Import-Module .\powershell-yaml


$workDir = Get-Location

$conf = Get-Content "config.yml" | ConvertFrom-Yaml
$targets = Get-Content $conf.対象ホスト一覧 | ConvertFrom-Csv | foreach { "$($_.アドレス)" }

$conf.対象ホスト = $targets

$conf.ログ保存先 = (Resolve-Path $conf.ログ保存先).Path
$conf.結果保存先 = (Resolve-Path $conf.結果保存先).Path

if ($conf.ステップ.Count -eq 0) {
    Write-Error "実行するステップが設定されていません。" -ErrorAction Stop
}
foreach ($i in 0..($conf.ステップ.Count - 1)) {
    $step = $conf.ステップ[$i]

    if ($step.配布 -and $step.回収) {
        Write-Error "$($i + 1)つめのステップが不正です: 1つのステップに配布と回収の両方を含めることは出来ません。" -ErrorAction Stop
    } elseif ($step.配布) {
        if (-not $step.配布.ファイル) {
            Write-Error "$($i + 1)つめのステップが不正です: ファイルが設定されていません。" -ErrorAction Stop
        }
        if (-not $step.配布.宛先) {
            Write-Error "$($i + 1)つめのステップが不正です: 宛先が設定されていません。" -ErrorAction Stop
        }
    } elseif ($step.回収) {
        if (-not $step.回収.ファイル) {
            Write-Error "$($i + 1)つめのステップが不正です: ファイルが設定されていません。" -ErrorAction Stop
        }
        if (-not $step.回収.宛先) {
            Write-Error "$($i + 1)つめのステップが不正です: 宛先が設定されていません。" -ErrorAction Stop
        }
    } else {
        Write-Error "$($i + 1)つめのステップが不正です: ステップには配布もしくは回収を指定する必要があります。" -ErrorAction Stop
    }
}

if ($conf.並列実行数 -lt 1) {
    Write-Warning "並列実行数は1未満に出来ません。1として実行します。"
    $conf.並列実行数 = 1
}
if ($conf.最大試行回数 -lt 1) {
    Write-Warning "最大試行回数は1未満に出来ません。1として実行します。"
    $conf.最大試行回数 = 1
}

Write-Host "タスク: $($conf.タスク名) （$($conf.ステップ.Count)ステップ）"
Write-Host "対象ホスト: $($conf.対象ホスト一覧) （$($targets.Count)ホスト）"
Write-Host "並列実行数: $($conf.並列実行数)ホスト  最大試行回数: $($conf.最大試行回数)回まで"
Write-Host "ログ保存先: $($conf.ログ保存先)"
Write-Host "結果保存先: $($conf.結果保存先)"

$credential = Get-Credential $conf.ユーザ名 -ErrorAction Stop
if (-not $credential) {
    exit
}

$pool = New-TPTaskPool -NumSlots $conf.並列実行数

foreach ($t in $targets) {
    Add-TPTask                              `
        -TaskPool $pool                     `
        -Name "$($conf.タスク名)_${t}"      `
        -MaxRetry ($conf.最大試行回数 - 1)  `
        -Arguments @($t, $conf, $workDir)   `
        -Action {
            param([string]$address, [Hashtable]$conf, [string]$workDir)

            ping -n 1 $address | Out-Null
            if ($LastExitCode -ne 0) {
                throw "ホストに接続できませんでした"
            }

            Set-Location $workDir

            foreach ($step in $conf.ステップ) {
                if ($step.配布) {
                    $path = "\\${address}\$($step.配布.宛先 -replace "(^[a-zA-Z]):","`$1$")"

                    New-PSDrive FileDstributor -PSProvider FileSystem -Root $path -Credential $credential -ErrorAction Stop

                    if (-not (Test-Path -Type Container "FileDstributor:/")) {
                        throw "対象フォルダに接続できませんでした"
                    }

                    Copy-Item $step.配布.ファイル "FileDstributor:/"

                    Remove-PSDrive FileDstributor
                } else {
                    $path = "\\${address}\$((Split-Path $step.回収.ファイル) -replace "(^[a-zA-Z]):","`$1$")"
                    New-PSDrive FileDstributor -PSProvider FileSystem -Root $path -Credential $credential -ErrorAction Stop

                    if (-not (Test-Path -Type Container "FileDstributor:/")) {
                        throw "対象フォルダに接続できませんでした"
                    }

                    $fname = "FileDstributor:/$(Split-Path -Leaf $step.回収.ファイル)"
                    if (-not (Test-Path $fname)) {
                        throw "回収ファイル `"${file}`" が見つかりません"
                    }

                    $targetDir = $step.回収.宛先 -replace "HOST_ADDRESS",$address
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
        } | Out-Null
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

if ($MyInvocation.InvocationName -eq "&") {
    pause
}
