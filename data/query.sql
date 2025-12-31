-- 外部テーブルを登録
CREATE EXTERNAL TABLE my_table
STORED AS JSON
LOCATION 'data/*.jsonl';

-- Bobという名前のデータを抽出
SELECT * FROM my_table WHERE name = 'Alice';
