# DataFusion SQL Processor

このプロジェクトは、Apache DataFusion を使用して JSONL (Newline Delimited JSON) ファイルに対して SQL クエリを実行し、結果を CSV 形式で出力する Rust サンプルプログラムです。

`rust_dev` フォルダ内の Docker 環境を使用してビルド・実行します。

## プロジェクト構成

- `src/main.rs`: SQLファイル内のDDLおよびクエリを実行し、CSVを出力するロジック。
- `data/query.sql`: テーブル定義（CREATE EXTERNAL TABLE）と実行クエリ。
- `data/input.jsonl`: 入力用サンプルデータ。
- `rust_dev/`: Rust 開発用 Docker 環境。

## 使い方

### 1. コンテナの起動

`rust_dev` ディレクトリに移動し、Docker コンテナをバックグラウンドで起動します。

```bash
cd rust_dev
docker compose up -d --build
```

### 2. コンテナ内でのビルドと実行

起動したコンテナに入り、プロジェクトディレクトリに移動して Rust のコマンドを実行します。

```bash
# コンテナに入る
docker exec -it rustdev bash

# プロジェクトディレクトリに移動
cd /work/dev/project/csv-gz2csv/datafusion-sql-processor

# 実行例
cargo run -- data/query.sql
cargo run -- data/query_csv.sql
cargo run -- data/query_csv_gz.sql

# 出力先ディレクトリのベースを指定する場合 (例: output_test/output/ に出力)
cargo run -- data/query.sql output_test
```

### 3. Linux バイナリのビルドと出力

コンテナ内で Linux 用のバイナリをビルドし、ホスト側にコピーする手順です。

```bash
# Release ビルドを実行
cargo build --release

# ビルド生成物をホスト側にコピー
# (ビルド生成物はコンテナ内ローカルの /home/debian/target に出力されます)
cp /home/debian/target/release/datafusion-sql-processor .
```

### 4. クロスコンパイル（Windows 用）

`cargo-zigbuild` を使用して、コンテナ内で Windows 用の `.exe` バイナリをビルドできます。

```bash
# Windows x86_64 用にビルド
cargo zigbuild --target x86_64-pc-windows-gnu --release

# ビルド生成物をホスト側にコピー
# (ビルド生成物はコンテナ内ローカルの /home/debian/target に出力されます)
cp /home/debian/target/x86_64-pc-windows-gnu/release/datafusion-sql-processor.exe .
```

### 5. 一括処理 (Batch Processing)

ディレクトリ内の複数の JSONL ファイルを一括で CSV に変換するためのスクリプトを用意しています。これらのスクリプトは、ベースとなる SQL ファイルの `LOCATION` を動的に書き換えて実行します。

### Linux / macOS (Bash)

```bash
chmod +x process_jsonl.sh
./process_jsonl.sh <jsonlのあるディレクトリ> <ベースSQLファイル>

# 例: data フォルダ内の全 jsonl を data/query.sql をベースに処理
./process_jsonl.sh data data/query.sql
```

### Windows (Batch)

```powershell
# PowerShell の場合は .\ を付けて実行してください
.\process_jsonl.bat <jsonlのあるディレクトリ> <ベースSQLファイル>

# 例:
.\process_jsonl.bat data data\query.sql
```

変換された CSV は `output_csv/<ファイル名>/` ディレクトリ内に出力されます。

## トラブルシューティング

### Permission Denied が発生する場合

ホスト側のユーザーID (UID) とグループID (GID) がコンテナ内のユーザーと一致していない可能性があります。
以下の手順でコンテナを再ビルドしてください。

1. ホスト側で UID/GID を確認します：
   
   ```bash
   id -u  # UID の確認
   id -g  # GID の確認
   ```

2. 取得した値を指定して再ビルドします：
   
   ```bash
   cd rust_dev
   docker compose build --build-arg UID=$(id -u) --build-arg GID=$(id -g)
   docker compose up -d
   ```

> [!NOTE]
> 現在の設定では `target` ディレクトリはコンテナ内のローカル領域 (`/home/debian/target`) に出力されるため、ホスト側のマウントボリュームにおける権限競合は発生しません。
