Import-Module .\TaskPool
Import-Module .\powershell-yaml


function ConvertFrom-Configuration {
    $conf = $input | ConvertFrom-Yaml
    $conf.対象ホスト = foreach ($h in (Get-Content $conf.対象ホスト一覧 | ConvertFrom-Csv)) {
        "$($h.アドレス)"
    }

    $conf.ログ保存先 = (Resolve-Path $conf.ログ保存先).Path
    $conf.結果保存先 = (Resolve-Path $conf.結果保存先).Path

    if ($conf.ステップ.Count -eq 0) {
        Write-Error "実行するステップが設定されていません。" -ErrorAction Stop
    }
    foreach ($i in 0..($conf.ステップ.Count - 1)) {
        $step = $conf.ステップ[$i]

        if ($step.Count -ne 1) {
            Write-Error "$($i + 1)つめのステップが不正です: 1つのステップには1つの指示を設定する必要があります。" -ErrorAction Stop
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
        } elseif ($step.ハッシュ取得) {
            if (-not $step.ハッシュ取得.ファイル) {
                Write-Error "$($i + 1)つめのステップが不正です: ファイルが設定されていません。" -ErrorAction Stop
            }
            if (-not $step.ハッシュ取得.保存先) {
                Write-Error "$($i + 1)つめのステップが不正です: 保存先が設定されていません。" -ErrorAction Stop
            }
        } else {
            Write-Error "$($i + 1)つめのステップが不正です: 不明な指示 `"$($step.Keys)`" が設定されています。" -ErrorAction Stop
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

    $conf
}

function New-StatusReporter($Conf) {
    $status = [PSCustomObject]@{
        Executed = 0
        Completed = 0
        Errored = 0
        SuccessHosts = @{}
        TryCount= @{}
    }

    $writeStatus = {
        Write-Progress "$($Conf.タスク名)" "$($Conf.対象ホスト.Count)ホスト中$($status.Completed)ホスト完了 （$($status.Executed)回実行 : $($status.Completed)回成功 : $($status.Errored)回失敗）" -Id 0 -PercentComplete ($status.Completed * 100 / $Conf.対象ホスト.Count)
    }.GetNewClosure()

    $writeLog = {
        param($hostAddress, $errorReason)

        [PSCustomObject]@{
            日時 = Get-Date
            タスク名 = "$($Conf.タスク名)"
            ターゲットホスト = "$($hostAddress)"
            結果 = if ($errorReason -eq $null) { "成功" } else { "失敗" }
            理由 = $errorReason
            成功数 = $status.Completed
            失敗数 = $status.Errored
            実行数 = $status.Executed
            対象ホスト数 = $Conf.対象ホスト.Count
        } | Export-Csv $Conf.ログ保存先 -NoTypeInformation -Append -Encoding Default
    }.GetNewClosure()

    & $writeStatus $status

    [PSObject]@{
        OnTaskComplete = {
            $target = $_.Task.Arguments[0]

            $status.Executed += 1
            $status.Completed += 1
            $status.SuccessHosts[$target] = 1
            $status.TryCount[$target] += 1

            & $writeStatus
            & $writeLog $target $null
        }.GetNewClosure()

        OnTaskError = {
            $target = $_.Task.Arguments[0]

            $status.Executed += 1
            $status.Errored += 1
            $status.TryCount[$target] += 1

            & $writeStatus
            & $writeLog $target $_.Error

            Write-Warning "${target}: $($_.Error)"
        }.GetNewClosure()

        OnAllDone = {
            $Conf.対象ホスト | foreach {
                [PSCustomObject]@{
                    ターゲットホスト = $_
                    結果 = if ($status.SuccessHosts.ContainsKey($_)) { "成功" } else { "失敗" }
                    試行回数 = $status.TryCount[$_]
                }
            } | Export-Csv $Conf.結果保存先 -NoTypeInformation -Encoding Default
        }.GetNewClosure()
    }
}


$Task = {
    param([string]$address, [Hashtable]$conf, [string]$workDir, [PSCredential]$credential)

    $mount = {
        param([string]$TargetPath)

        $path = $TargetPath -replace "(^[a-zA-Z]):","`$1$" -replace "\\$",""
        $path = "\\${address}\${path}"

        New-PSDrive FileDistoributor -PSProvider FileSystem -Root $path -Credential $credential -Scope 1 -ErrorAction Stop | Out-Null

        if (-not (Test-Path -Type Container "FileDistoributor:/")) {
            throw "対象フォルダに接続できませんでした"
        }
    }

    ping -n 1 $address | Out-Null
    if ($LastExitCode -ne 0) {
        throw "ホストに接続できませんでした"
    }

    Set-Location $workDir

    foreach ($step in $conf.ステップ) {
        if ($step.配布) {
            & $mount $step.配布.宛先

            Copy-Item $step.配布.ファイル "FileDistoributor:/"
        } elseif ($step.配布) {
            & $mount (Split-Path $step.回収.ファイル)

            $fname = "FileDistoributor:/$(Split-Path -Leaf $step.回収.ファイル)"
            if (-not (Test-Path $fname)) {
                throw "回収対象の `"$($step.回収.ファイル)`" が見つかりません"
            }

            $targetDir = $step.回収.宛先 -replace "HOST_ADDRESS",$address
            if (-not (Test-Path $targetDir)) {
                mkdir $targetDir
            }

            if (Test-Path -Type Leaf $fname) {
                Copy-Item $fname $targetDir | Out-Null
            } else {
                $target = "${targetDir}/$(Split-Path -Leaf $fname).csv"
                Get-ChildItem $fname | select -Property Name,Length,Mode,CreationTime,LastWriteTime,LastAccessTime | Export-Csv $target -NoTypeInformation -Encoding Default
            }
        } elseif ($step.ハッシュ取得) {
            & $mount (Split-Path $step.ハッシュ取得.ファイル)

            $fname = "FileDistoributor:/$(Split-Path -Leaf $step.ハッシュ取得.ファイル)"
            if (-not (Test-Path $fname)) {
                throw "ハッシュ取得対象の `"$($step.ハッシュ取得.ファイル)`" が見つかりません"
            }

            Get-FileHash -Algorithm SHA256 $fname | foreach {
                [PSCustomObject]@{
                    ホスト = $address
                    ファイル名 = "$(Split-Path $step.ハッシュ取得.ファイル)\$(Split-Path -Leaf $_.Path)"
                    ハッシュ値 = $_.Hash
                }
            } | Export-Csv $step.ハッシュ取得.保存先 -NoTypeInformation -Append -Encoding Default
        }

        Remove-PSDrive FileDistoributor -ErrorAction Ignore
    }
}


if ($MyInvocation.InvocationName -ne ".") {
    $conf = Get-Content "config.yml" | ConvertFrom-Configuration
    $workDir = Get-Location

    Write-Host "タスク: $($conf.タスク名) （$($conf.ステップ.Count)ステップ）"
    Write-Host "対象ホスト: $($conf.対象ホスト一覧) （$($conf.対象ホスト.Count)ホスト）"
    Write-Host "並列実行数: $($conf.並列実行数)ホスト  最大試行回数: $($conf.最大試行回数)回まで"
    Write-Host "ログ保存先: $($conf.ログ保存先)"
    Write-Host "結果保存先: $($conf.結果保存先)"

    $credential = Get-Credential $conf.ユーザ名 -ErrorAction Stop
    if (-not $credential) {
        exit
    }

    $reporter = New-StatusReporter $conf
    $pool = New-TPTaskPool -NumSlots $conf.並列実行数               `
                           -OnTaskComplete $reporter.OnTaskComplete `
                           -OnTaskError $reporter.OnTaskError

    foreach ($t in $conf.対象ホスト) {
        Add-TPTask -TaskPool $pool                                `
                   -Name "$($conf.タスク名)_${t}"                 `
                   -MaxRetry ($conf.最大試行回数 - 1)             `
                   -Arguments @($t, $conf, $workDir, $credential) `
                   -Action $Task | Out-Null
    }

    try {
        $pool.Run()
    } finally {
        & $reporter.OnAllDone
    }

    if ($MyInvocation.InvocationName -eq "&") {
        pause
    }
}
