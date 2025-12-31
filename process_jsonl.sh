#!/bin/bash

# Usage: ./process_jsonl.sh <input_dir> <base_sql_file>
# Example: ./process_jsonl.sh data data/query.sql

INPUT_DIR=$1
BASE_SQL=$2

if [ -z "$INPUT_DIR" ] || [ -z "$BASE_SQL" ]; then
    echo "Usage: $0 <input_dir> <base_sql_file>"
    exit 1
fi

# Ensure output directory exists
OUTPUT_ROOT="output_csv"
mkdir -p "$OUTPUT_ROOT"

# Iterate over .jsonl files
for jsonl_file in "$INPUT_DIR"/*.jsonl; do
    if [ ! -f "$jsonl_file" ]; then
        echo "No .jsonl files found in $INPUT_DIR"
        continue
    fi

    filename=$(basename "$jsonl_file")
    basename="${filename%.*}"
    echo "Processing $jsonl_file ..."

    # Create a temporary SQL file with updated LOCATION
    TEMP_SQL="temp_${basename}.sql"
    
    # Read base SQL and replace the LOCATION line. 
    # Assumes the location to be replaced is in the CREATE EXTERNAL TABLE statement.
    # Note: This is a simple replacement logic.
    sed "s|LOCATION '.*'|LOCATION '$jsonl_file'|g" "$BASE_SQL" > "$TEMP_SQL"

    # Run the processor
    ./datafusion-sql-processor "$TEMP_SQL" "$OUTPUT_ROOT/$basename"

    # Clean up
    rm "$TEMP_SQL"
done

echo "Done. Results are in $OUTPUT_ROOT"
