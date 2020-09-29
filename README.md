FileDistributor
===============

多数のホストに一気にファイルをばらまく・回収する。


## 使い方

### 1. 設定ファイルを作る

`config.yml` の名前で以下のような設定ファイルを作ります。

``` yaml
タスク名: "一括集配テスト"

並列実行数: 5    # 同時に実行するタスクの数。
最大試行回数: 1  # 1つのホストあたりの最大試行回数。

ログ保存先: "log.csv"     # ログを保存するファイル名。
結果保存先: "result.csv"  # 結果を保存するファイル名。

対象ホスト一覧: "target.csv"  # 配布対象のホストの一覧。

ユーザ名: "Administrator"  # 対象ホストにログインする際に使うユーザ名。

# 実行するタスクの中身を以下に記述します。
# 各ステップには、配布もしくは回収を設定できます。
ステップ:
  - 配布:
      ファイル: "配布物/hosts/*"  # 配布したいファイルの名前。アスタリスクで複数のファイルを配布できます。
      宛先: "C:/Windows/System32/drivers/etc/"
  - 回収:
      ファイル: "C:/Users"  # 回収対象がフォルダの場合、配下のファイルの一覧がCSV形式で保存されます。
      宛先: "回収物/HOST_ADDRESS/"
```


### 2. 対象ホストの一覧を作る

`対象ホスト一覧` で指定したファイル名で、対象ホストの一覧を作ります。

``` csv
アドレス
111.222.333.444
123.234.123.234
```


### 3. 実行する

`FileDistrobutor.ps1` を右クリックして「PowerShellで実行」を選ぶか、シェルから以下のコマンドで実行します。

``` powershell
PS> ./FileDistrobutor.ps1
```

実行すると `ユーザ名` で指定したユーザのパスワードを聞かれるので、入力します。


### 4. 結果を確認する

実行中に `ログ保存先` で指定した場所にログが出力されます。
また、実行が完了すると `結果保存先` で指定した場所に各ホストの実行結果が保存されます。
