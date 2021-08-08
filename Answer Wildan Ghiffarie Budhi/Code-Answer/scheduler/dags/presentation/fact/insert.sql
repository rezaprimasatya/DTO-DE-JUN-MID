INSERT IGNORE INTO presentation.{{ params.tablename }}
{% include params.query_file_path %}