from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    "presentation_pipeline",
    start_date=datetime(2021, 8, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    # ========================================= Sensors =========================================

    dag_start = ExternalTaskSensor(
        task_id="presentation_pipeline_start",
        external_dag_id="standardized_pipeline",
        external_task_id="standardized_pipeline_end"
    )

    # ========================================= CREATE SCHEMA =========================================
    
    # DIMENSION TABLE

    nations_dim_schema = MySqlOperator(
        task_id='create_presentation_table_dim_nations',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/nations/create_table.sql'
    )   

    customers_dim_schema = MySqlOperator(
        task_id='create_presentation_table_dim_customers',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/customers/create_table.sql',
    )

    dates_dim_schema = MySqlOperator(
        task_id='create_presentation_table_dim_dates',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/dates/create_table.sql',
    )

    parts_dim_schema = MySqlOperator(
        task_id='create_presentation_table_dim_parts',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/parts/create_table.sql',
    )

    suppliers_dim_schema = MySqlOperator(
        task_id='create_presentation_table_dim_suppliers',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/suppliers/create_table.sql',
    )

    # FACT TABLE

    order_line_items_fact_schema = MySqlOperator(
        task_id='create_presentation_table_fact_order_line_items',
        mysql_conn_id='dwh_id',
        sql='./presentation/fact/order_line_items/create_table.sql',
    )

    # ========================================= PRESENTATION =========================================

    # DIMENSION TABLE

    nations_dim_insert = MySqlOperator(
        task_id='presentation_dim_nations',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/insert.sql', 
        params={"tablename": "nations", "query_file_path": "presentation/dimension/nations/query.sql"}  
    )

    customers_dim_insert = MySqlOperator(
        task_id='presentation_dim_customers',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/insert.sql', 
        params={"tablename": "customers", "query_file_path": "presentation/dimension/customers/query.sql"}  
    )

    dates_dim_insert = MySqlOperator(
        task_id='presentation_dim_dates',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/insert.sql', 
        params={"tablename": "dates", "query_file_path": "presentation/dimension/dates/query.sql"}  
    )

    parts_dim_insert = MySqlOperator(
        task_id='presentation_dim_parts',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/insert.sql', 
        params={"tablename": "parts", "query_file_path": "presentation/dimension/parts/query.sql"}  
    )

    suppliers_dim_insert = MySqlOperator(
        task_id='presentation_dim_suppliers',
        mysql_conn_id='dwh_id',
        sql='./presentation/dimension/insert.sql', 
        params={"tablename": "suppliers", "query_file_path": "presentation/dimension/suppliers/query.sql"}  
    )

    # FACT TABLE

    order_line_items_fact_insert = MySqlOperator(
        task_id='presentation_fact_order_line_items',
        mysql_conn_id='dwh_id',
        sql='./presentation/fact/insert.sql', 
        params={"tablename": "order_line_items", "query_file_path": "presentation/fact/order_line_items/query.sql"}  
    )

    # ========================================= DAG ORDER =========================================

    dag_start >> [
        nations_dim_schema,
        customers_dim_schema,
        dates_dim_schema,
        parts_dim_schema,
        suppliers_dim_schema
    ]

    [
        nations_dim_schema >> nations_dim_insert,
        customers_dim_schema >> customers_dim_insert,
        dates_dim_schema >> dates_dim_insert,
        parts_dim_schema >> parts_dim_insert,
        suppliers_dim_schema >> suppliers_dim_insert
    ]
    
    [
        nations_dim_insert,
        customers_dim_insert,
        dates_dim_insert,
        parts_dim_insert,
        suppliers_dim_insert
    ] >> order_line_items_fact_schema >> order_line_items_fact_insert