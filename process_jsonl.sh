#!/bin/bash

# Usage: ./process_jsonl.sh <input_dir> <base_sql_file> <format>
# Example: ./process_jsonl.sh data data/query.sql jsonl

INPUT_DIR=$1
BASE_SQL=$2
FORMAT=${3:-jsonl}

if [ -z "$INPUT_DIR" ] || [ -z "$BASE_SQL" ]; then
    echo "Usage: $0 <input_dir> <base_sql_file> <format>"
    exit 1
fi

# 出力ディレクトリの作成
OUTPUT_ROOT="output_$FORMAT"
mkdir -p "$OUTPUT_ROOT"

# .jsonl ファイルをループ処理
for jsonl_file in "$INPUT_DIR"/*.jsonl; do
    if [ ! -f "$jsonl_file" ]; then
        echo "No .jsonl files found in $INPUT_DIR"
        continue
    fi

    filename=$(basename "$jsonl_file")
    basename="${filename%.*}"
    echo "Processing $jsonl_file ..."

    # 一時SQLファイルの作成
    TEMP_SQL="${basename}.sql"
    
    # sedを使用してLOCATION句を入力ファイルパスに置換
    sed "s|LOCATION '.*'|LOCATION '$jsonl_file'|g" "$BASE_SQL" > "$TEMP_SQL"

    # プロセッサの実行 (-f でフォーマットを指定)
    ./datafusion-sql-processor "$TEMP_SQL" -f "$FORMAT" -o "$OUTPUT_ROOT"

    # 後片付け
    rm "$TEMP_SQL"
done

echo "Done. Results are in $OUTPUT_ROOT"