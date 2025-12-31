CREATE EXTERNAL TABLE csv_gz_table (
    "所有者コード" VARCHAR,
    "所有者名" VARCHAR,
    "住所" VARCHAR
)
STORED AS CSV
LOCATION 'data/input.csv.gz'
OPTIONS (
    format.has_header true,
    format.compression gzip
);

-- 30歳以上のデータを抽出
SELECT * FROM csv_gz_table WHERE 所有者コード='06520';

