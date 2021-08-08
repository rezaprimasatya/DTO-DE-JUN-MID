CREATE TABLE IF NOT EXISTS standardized.{{ params.tablename }} (INGESTION_TIMESTAMP TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP) AS
{% include params.query_file_path %}
LIMIT 0