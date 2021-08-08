from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.external_task import ExternalTaskSensor, ExternalTaskMarker

default_args = {
    'owner': 'airflow',
    'retries': 10,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    "standardized_pipeline",
    start_date=datetime(2021, 8, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args
) as dag:

    # ========================================= Sensors =========================================

    dag_start = ExternalTaskSensor(
        task_id="standardized_pipeline_start",
        external_dag_id="to_datalake_pipeline",
        external_task_id="to_datalake_pipeline_end"
    )

    dag_end = ExternalTaskMarker(
        task_id="standardized_pipeline_end",
        external_dag_id="presentation_pipeline",
        external_task_id="presentation_pipeline_start",
    )


    # ========================================= CREATE SCHEMA =========================================
    
    regions_schema = MySqlOperator(
        task_id='create_standardized_table_regions',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "regions", "query_file_path": "standardized/tables/regions.sql"}
    )

    nations_schema = MySqlOperator(
        task_id='create_standardized_table_nations',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "nations", "query_file_path": "standardized/tables/nations.sql"}
    )

    parts_schema = MySqlOperator(
        task_id='create_standardized_table_parts',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "parts", "query_file_path": "standardized/tables/parts.sql"}
    )

    suppliers_schema = MySqlOperator(
        task_id='create_standardized_table_suppliers',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "suppliers", "query_file_path": "standardized/tables/suppliers.sql"}
    )

    part_suppliers_schema = MySqlOperator(
        task_id='create_standardized_table_part_suppliers',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "part_suppliers", "query_file_path": "standardized/tables/part_suppliers.sql"}
    )

    customers_schema = MySqlOperator(
        task_id='create_standardized_table_customers',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "customers", "query_file_path": "standardized/tables/customers.sql"}
    )

    orders_schema = MySqlOperator(
        task_id='create_standardized_table_orders',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "orders", "query_file_path": "standardized/tables/orders.sql"}
    )

    line_items_schema = MySqlOperator(
        task_id='create_standardized_table_line_items',
        mysql_conn_id='dwh_id',
        sql='./standardized/create_table.sql', 
        params={"tablename": "line_items", "query_file_path": "standardized/tables/line_items.sql"}
    )

    # ========================================= STANDARDIZATION =========================================

    regions_insert = MySqlOperator(
        task_id='standardized_regions',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "regions", "query_file_path": "standardized/tables/regions.sql"}  
    )

    nations_insert = MySqlOperator(
        task_id='standardized_nations',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "nations", "query_file_path": "standardized/tables/nations.sql"}  
    )

    parts_insert = MySqlOperator(
        task_id='standardized_parts',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "parts", "query_file_path": "standardized/tables/parts.sql"}  
    )

    suppliers_insert = MySqlOperator(
        task_id='standardized_suppliers',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "suppliers", "query_file_path": "standardized/tables/suppliers.sql"}  
    )

    part_suppliers_insert = MySqlOperator(
        task_id='standardized_part_suppliers',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "part_suppliers", "query_file_path": "standardized/tables/part_suppliers.sql"}  
    )

    customers_insert = MySqlOperator(
        task_id='standardized_customers',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "customers", "query_file_path": "standardized/tables/customers.sql"}  
    )

    orders_insert = MySqlOperator(
        task_id='standardized_orders',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "orders", "query_file_path": "standardized/tables/orders.sql"}  
    )

    line_items_insert = MySqlOperator(
        task_id='standardized_line_items',
        mysql_conn_id='dwh_id',
        sql='./standardized/insert.sql', 
        params={"tablename": "line_items", "query_file_path": "standardized/tables/line_items.sql"}  
    )
     
    # ========================================= DAG ORDER =========================================

    dag_start >> [
        regions_schema,
        nations_schema,
        parts_schema,
        suppliers_schema,
        part_suppliers_schema,
        customers_schema,
        orders_schema,
        line_items_schema
    ]

    [
        regions_schema >> regions_insert,
        nations_schema >> nations_insert,
        parts_schema >> parts_insert,
        suppliers_schema >> suppliers_insert,
        part_suppliers_schema >> part_suppliers_insert,
        customers_schema >> customers_insert,
        orders_schema >> orders_insert,
        line_items_schema >> line_items_insert
    ] >> dag_end
