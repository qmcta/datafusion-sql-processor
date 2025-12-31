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

# 基本的な実行（デフォルトで CSV 出力、SQLと同じ階層の output/ フォルダに保存）
cargo run -- data/query.sql 

# 出力ディレクトリを指定する場合
cargo run -- data/query.sql --output-dir ./my_output 

# 出力フォーマットを指定する場合 (csv, parquet, jsonl)
cargo run -- data/query.sql --format jsonl
```

コマンドライン引数詳細

- `sql_file_path`: 実行するSQLファイルのパス（必須）。

- `-f, --format`: 出力形式 (`csv`, `parquet`, `jsonl`)。デフォルトは `csv`。

- `-o, --output-dir`: 出力先ディレクトリ。未指定の場合はSQLファイルと同じ階層の `output` フォルダが自動作成されます。

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

ディレクトリ内の複数の `.jsonl` ファイルを同じ SQL テンプレートで一括処理するために、ヘルパースクリプトが用意されています 。これらのスクリプトは、入力ファイル名に基づいて一時的な SQL ファイルを生成し、個別の出力ファイルを生成します。

スクリプトの引数

1. `input_dir`: `.jsonl` ファイルが格納されているディレクトリ。

2. `base_sql_file`: 実行する SQL ファイル（内部に `LOCATION '...'` 句を含む必要があります）。

3. `format`: 出力形式 (`csv`, `parquet`, `jsonl`)。デフォルトは `jsonl`。

Linux / macOS (Bash)

```bash
chmod +x process_jsonl.sh
./process_jsonl.sh <input_dir> <base_sql_file> <format>

# 例: data フォルダ内の全 jsonl を parquet 形式で一括処理
./process_jsonl.sh data data/query.sql parquet
```

Windows (Batch)

```powershell
process_jsonl.bat <input_dir> <base_sql_file> <format>

# 例: data フォルダ内の全 jsonl を jsonl 形式で一括処理
process_jsonl.bat data data\query.sql jsonl
```

スクリプトの動作詳細

- **出力先**: フォーマットに応じて `output_csv/`, `output_parquet/`, `output_jsonl/` ディレクトリが自動作成されます。

- **ファイル名**: 入力ファイル名（拡張子除く）がそのまま出力ファイル名として引き継がれます。

- **SQLの自動書き換え**: スクリプトは実行時、SQL ファイル内の `LOCATION '.*'` 部分を現在の入力ファイルのパスに自動的に置換します。

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
