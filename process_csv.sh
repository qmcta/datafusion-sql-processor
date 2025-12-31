#!/bin/bash

# Usage: ./process_csv.sh <input_dir> <base_sql_file> <format>
# Example: ./process_csv.sh data data/query.sql csv

INPUT_DIR=$1
BASE_SQL=$2
FORMAT=${3:-csv}

if [ -z "$INPUT_DIR" ] || [ -z "$BASE_SQL" ]; then
    echo "Usage: $0 <input_dir> <base_sql_file> <format>"
    exit 1
fi

# 出力ディレクトリの作成
OUTPUT_ROOT="output_$FORMAT"
mkdir -p "$OUTPUT_ROOT"

# .csv ファイルをループ処理
for csv_file in "$INPUT_DIR"/*.csv; do
    if [ ! -f "$csv_file" ]; then
        echo "No .csv files found in $INPUT_DIR"
        continue
    fi

    filename=$(basename "$csv_file")
    basename="${filename%.*}"
    echo "Processing $csv_file ..."

    # 一時SQLファイルの作成
    TEMP_SQL="temp_${basename}.sql"
    
    # sedを使用してLOCATION句を入力ファイルパスに置換
    sed "s|LOCATION '.*'|LOCATION '$csv_file'|g" "$BASE_SQL" > "$TEMP_SQL"

    # プロセッサの実行 (-f でフォーマットを指定)
    ./datafusion-sql-processor "$TEMP_SQL" -f "$FORMAT" -o "$OUTPUT_ROOT"

    # 後片付け
    rm "$TEMP_SQL"
done

echo "Done. Results are in $OUTPUT_ROOT"