CREATE EXTERNAL TABLE csv_table (
    "所有者コード" VARCHAR,
    "所有者名" VARCHAR,
    "住所" VARCHAR
)
STORED AS CSV
LOCATION 'data/input.csv'
OPTIONS (format.has_header true);

-- 全データを抽出
SELECT * FROM csv_table;
