# Specification: DataFusion SQL Processor (`spec.md`)

## 1. Overview
`datafusion-sql-processor` は、Apache DataFusion クエリエンジンを利用して、外部 SQL ファイルに記述されたテーブル定義（DDL）およびクエリ（DML）をインメモリ・カラムナ処理で実行し、指定フォーマット（CSV / Parquet / JSONL）へ結果を出力する Rust ベースの CLI ツールです。

---

## 2. 目的とスコープ
- **外部 SQL による完全制御**: ソースコードを再コンパイルすることなく、SQL ファイルのみでデータソースの登録 (`CREATE EXTERNAL TABLE`)、スキーマ定義、クエリ抽出 (`SELECT`)、集計条件の変更を可能とする。
- **多様なフォーマット・圧縮対応**: CSV、JSONL、Parquet を透過的に扱い、Gzip 圧縮されたファイル (`.csv.gz`, `.jsonl.gz`) の直接読み込みをサポートする。
- **ストリーミング & バッチ連携**: 単体実行から、ディレクトリ単位での一括処理スクリプト（`process_*.sh` / `process_*.bat`）との連携をサポートする。

---

## 3. システムアーキテクチャ

```
[SQL File (.sql)] ------------+
                              |
[Input Files (.jsonl/.csv)] --+--> [datafusion-sql-processor (Rust / DataFusion 51.x)]
                                   |
                                   +--> Parse Statements (';' 分割)
                                   +--> Register DDLs into SessionContext
                                   +--> Execute Final DML Statement
                                   +--> Stdout Preview (df.show())
                                   +--> Export to [Output Directory] (.csv / .parquet / .jsonl)
```

---

## 4. CLI インターフェース仕様

### 4.1 コマンド構文
```bash
datafusion-sql-processor [OPTIONS] <SQL_FILE_PATH>
```

### 4.2 コマンドライン引数 (Arguments & Flags)

| 引数 / フラグ | 型 | 必須 / 任意 | デフォルト値 | 説明 |
| :--- | :--- | :--- | :--- | :--- |
| `<SQL_FILE_PATH>` | `String` (Path) | **必須** | - | 実行対象の SQL ファイルパス（DDL とクエリを記述） |
| `-f, --format` | `Enum` | 任意 | `csv` | 出力ファイルフォーマット (`csv`, `parquet`, `jsonl`) |
| `-o, --output-dir` | `PathBuf` | 任意 | `<SQL_PARENT>/output` | 出力先ディレクトリ。未指定時は SQL ファイルの親ディレクトリ配下の `output` フォルダ |
| `-h, --help` | Flag | 任意 | - | ヘルプ情報の表示 |
| `-V, --version` | Flag | 任意 | - | バージョン情報の表示 |

---

## 5. 処理フローと機能要件

### 5.1 入力検証とパス解決
1. **SQL ファイル検証**:
   - `<SQL_FILE_PATH>` の存在を確認する。存在しない場合は標準エラー出力（stderr）に `"SQL file not found: <PATH>"` を出力し、終了ステータスコード `1` で異常終了する。
2. **出力ディレクトリ決定**:
   - `--output-dir` が指定されている場合はそのパスを採用。
   - 未指定の場合は、SQL ファイルの親ディレクトリ（親が取得できない場合はカレントディレクトリ `.`）配下の `output` ディレクトリとする。
   - ディレクトリが存在しない場合、`fs::create_dir_all` により再帰的に作成する。
3. **出力ファイルパス命名**:
   - 出力ファイル名は `<SQLファイル名(拡張子なし)>.<フォーマット拡張子>` とする。
   - 例: `data/query.sql` かつ `--format jsonl` の場合、出力先は `<output_dir>/query.jsonl` となる。

### 5.2 SQL 解析と実行
1. **ステートメントの分割**:
   - SQL ファイル全体をテキストとして読み込み、セミコロン（`;`）で分割する。
   - 前後の空白・改行をトリムし、空ステートメントは除外する。
   - 有効なステートメントが 1 件も存在しない場合は stderr に `"No SQL statements found in <PATH>"` を出力し、終了ステータスコード `1` で異常終了する。
2. **順次実行**:
   - `datafusion::prelude::SessionContext` を初期化。
   - 抽出された各ステートメントを順番に `ctx.sql(stmt).await` で実行する。
   - 最後のステートメントの実行結果 (`DataFrame`) を出力用データフレーム (`final_df`) として保持する。

### 5.3 プレビュー出力とファイル保存
1. **プレビュー表示**:
   - 最終結果のデータフレームを標準出力（stdout）に `df.clone().show().await` でコンソール描画する。
2. **ファイル出力**:
   - 選択されたフォーマットに応じて保存処理を実行する。
     - `Csv`: `df.write_csv(output_file_path, DataFrameWriteOptions::default(), None).await`
     - `Parquet`: `df.write_parquet(output_file_path, DataFrameWriteOptions::default(), None).await`
     - `Jsonl`: `df.write_json(output_file_path, DataFrameWriteOptions::default(), None).await`
3. **完了通知**:
   - 成功時に stdout へ `"Successfully processed <SQL_PATH> and saved results to <OUTPUT_PATH>"` を出力し、正常終了（ステータスコード `0`）する。

---

## 6. 品質要件とテスト仕様

### 6.1 テスト要件
- **ユニットテスト / 結合テスト**:
  - `cargo test` にて全テストが PASS すること。
  - 引数パース、存在しない SQL ファイル指定時のエラー挙動、空ステートメント時のエラー挙動が正しくハンドリングされること。
- **静的解析・リンター**:
  - `cargo clippy --all-targets -- -D warnings` で警告がゼロであること。
  - `cargo fmt --check` によるフォーマット検証に準拠すること。

### 6.2 異常系ハンドリング基準
| 異常事象 | 期待される挙動 | 終了コード |
| :--- | :--- | :--- |
| SQL ファイルが存在しない | `SQL file not found: <path>` を stderr に出力 | 1 |
| SQL 内に有効なステートメントがない | `No SQL statements found in <path>` を stderr に出力 | 1 |
| 出力ディレクトリの作成に失敗 | `DataFusionError::Execution` を返し stderr に出力 | 1 |
| 不正な SQL 構文 / テーブル未定義 | エラー詳細および該当 SQL 文を stderr に出力 | 1 |

---

## 7. 検証コマンド一覧
```bash
# ビルド確認
cargo check

# ユニット・結合テスト
cargo test

# リンター検証
cargo clippy --all-targets -- -D warnings

# フォーマット検証
cargo fmt --check
```

