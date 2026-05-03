---
marp: true
theme: default
paginate: true
style: |
  section {
    font-family: 'Inter', 'Roboto', 'Segoe UI', sans-serif;
    color: #3c4043;
    background-color: #ffffff;
    font-size: 24px;
    padding: 40px;
  }
  h1 { color: #4285f4; border-bottom: 2px solid #4285f4; padding-bottom: 10px; }
  h2 { color: #1a73e8; }
  code { background-color: #f1f3f4; color: #d93025; border-radius: 4px; padding: 2px 6px; }
  pre { background-color: #202124; color: #f8f9fa; border-radius: 8px; padding: 20px; font-size: 18px; }
  .title-slide {
    background-image: linear-gradient(rgba(255,255,255,0.85), rgba(255,255,255,0.85)), url('tech_background.png');
    background-size: cover;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  .section-title { background-color: #4285f4; color: white; display: flex; align-items: center; justify-content: center; }
  .section-title h1 { color: white; border-bottom: none; font-size: 50px; }
  .badge { display: inline-block; padding: 4px 12px; border-radius: 16px; font-size: 14px; font-weight: bold; }
  .badge-yellow { background-color: #fef7e0; color: #b26a00; }
---

<!-- Title Slide -->
# DataFusion SQL Processor

Apache DataFusion + Rust で JSONL / CSV / Parquet 処理を行う Docker ベースのサンプル

---

<!-- Agenda -->
## Agenda

- プロジェクト構成
- 使い方: コンテナ起動・実行・バイナリ生成
- 一括処理スクリプト
- トラブルシューティング

---

<!-- Section Title -->
<section class="section-title">
# プロジェクト構成
</section>

---

## 何を含むか

- `src/main.rs`: SQL ファイルの DDL とクエリを実行し、CSV を出力する実装
- `data/query.sql`: CREATE EXTERNAL TABLE と実行クエリ
- `data/input.jsonl`: 入力サンプルデータ
- `rust_dev/`: Rust 開発用 Docker 環境

---

<section class="section-title">
# 使い方
</section>

---

## 1. コンテナの起動

```bash
cd rust_dev
docker compose up -d --build
```

- `rust_dev` フォルダ内の Docker 環境を使ってビルドと実行を安全に行う

---

## 2. コンテナ内でのビルドと実行

```bash
# コンテナに入る
docker exec -it rustdev bash

# プロジェクトディレクトリに移動
cd /work/dev/project/datafusion-sql-processor

# 基本実行
cargo run -- data/query.sql

# 出力ディレクトリ指定
cargo run -- data/query.sql --output-dir ./my_output

# 出力形式指定
cargo run -- data/query.sql --format jsonl
```

### コマンドライン引数

- `sql_file_path`: 実行する SQL ファイルのパス（必須）
- `-f, --format`: 出力形式 (`csv`, `parquet`, `jsonl`)、デフォルト `csv`
- `-o, --output-dir`: 出力先ディレクトリ

---

## 3. Linux バイナリのビルドと出力

```bash
# Release ビルド
git cargo build --release

# ホスト側にコピー
cp /home/debian/target/release/datafusion-sql-processor .
```

- ビルド成果物はコンテナ内 `/home/debian/target/release` に出力される

---

## 4. クロスコンパイル（Windows 用）

```bash
cargo zigbuild --target x86_64-pc-windows-gnu --release
cp /home/debian/target/x86_64-pc-windows-gnu/release/datafusion-sql-processor.exe .
```

- Windows `.exe` をコンテナ内で生成できる

---

<section class="section-title">
# 一括処理
</section>

---

## 5. JSONL 一括処理

- 複数 `.jsonl` ファイルを同じ SQL テンプレートで一括処理
- 入力ファイル名を元に一時 SQL を生成し、個別出力を作成

### Bash

```bash
chmod +x process_jsonl.sh
./process_jsonl.sh <input_dir> <base_sql_file> <format>

# 例:
./process_jsonl.sh data data/query.sql parquet
```

### Windows

```powershell
process_jsonl.bat <input_dir> <base_sql_file> <format>

# 例:
process_jsonl.bat data data\query.sql jsonl
```

---

## 6. 一括処理の動作

- 出力先: `output_csv/`, `output_parquet/`, `output_jsonl/`
- ファイル名: 入力ファイル名を引き継ぐ
- SQL 自動書き換え: `LOCATION '...'` 部分を入力ファイルパスに置換

---

## 7. CSV / CSV.GZ の一括処理

### CSV ファイル

```bash
./process_csv.sh <input_dir> <base_sql_file> <format>
# 例:
./process_csv.sh data data/query_csv.sql csv
```

### Windows

```powershell
process_csv.bat <input_dir> <base_sql_file> <format>
# 例:
process_csv.bat data data\query_csv.sql csv
```

---

## 8. CSV.GZ ファイル

### CSV.GZ

```bash
./process_csv_gz.sh <input_dir> <base_sql_file> <format>
# 例:
./process_csv_gz.sh data data/query_csv_gz.sql csv
```

### Windows

```powershell
process_csv_gz.bat <input_dir> <base_sql_file> <format>
# 例:
process_csv_gz.bat data data\query_csv_gz.sql csv
```

---

<section class="section-title">
# トラブルシューティング
</section>

---

## Permission Denied が発生する場合

- ホストの UID/GID とコンテナ内ユーザーが一致しない可能性

### 再ビルド手順

```bash
id -u  # UID の確認
id -g  # GID の確認

cd rust_dev
docker compose build --build-arg UID=$(id -u) --build-arg GID=$(id -g)
docker compose up -d
```

<span class="badge badge-yellow">NOTE</span> 現在の設定では `target` ディレクトリはコンテナ内ローカル `/home/debian/target` に出力されるため、ホスト側マウントの権限競合は発生しにくい。

---

<section class="section-title">
# Appendix
</section>

---

## 追加情報

- `data/query.sql`, `data/query_csv.sql`, `data/query_csv_gz.sql` の SQL 定義を変更して入力形式を切り替え
- `output_csv/`, `output_jsonl/` はサンプル出力フォルダ
- `process_*` スクリプトを使うことで、同じ SQL テンプレートを複数ファイルに適用できる

---

# Thank You

質問があればどうぞ。
