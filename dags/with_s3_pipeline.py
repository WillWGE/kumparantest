import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

source_tables = ['kumparan.article']

with DAG('with_s3_pipeline',
            schedule_interval=None, 
            catchup=False,
            template_searchpath=os.path.join(os.getcwd(), 'include', 'sql')
        ) as dag:

    @task(multiple_outputs=True)
    def get_dag_conf(**context):
        # dag_conf = Variable.get('customer', deserialize_json=True, default_var={})
        # load_type = dag_conf.get('load_type', None)
        # print(dag_conf)
        # print(context['dag_run'].run_type)
        load_type = Variable.get('load_type', default_var=None)
        print(f"Load Type: {load_type}")
        print(context['dag_run'].run_type)
        if context['dag_run'].run_type == "scheduled" and load_type == 'full':
            raise ValueError("Full run can't be scheduled!!! Might be left over from a previous run. Aborting...")
        
        if load_type == 'full':
            where_cond = None
        # elif load_type == 'delta':
        #     where_cond = " where updated_at > '{max_date}'"
        else:
            raise ValueError("Invalid load type")

        return {"where_cond": where_cond, "load_type": load_type}

    dag_conf = get_dag_conf()


    for source_table in source_tables:

        @task(task_id=f'{source_table}_extract')
        def extract(source_table: str, dag_conf: dict):
            pg_hook = PostgresHook(
                schema='postgres',
                postgres_conn_id='airflow_kumparan',
            )

            with open(os.path.join(os.getcwd(), 'include', 'sql', f'{source_table}.sql'), 'r') as sql_file:
                sql = sql_file.read()

            out_file = os.path.join(os.getcwd(), 'include', 'data', f'{source_table}.csv')
            if dag_conf['load_type'] == 'full':
                logging.info(f"Load Type: {dag_conf['load_type']}")
                sql = f"COPY {source_table} TO STDOUT WITH CSV DELIMITER ','"
            pg_hook.copy_expert(sql, out_file)
            
            s3_conn = S3Hook(aws_conn_id='aws')
            s3_conn.load_file(filename=out_file, key=f'pg_data/{source_table}.csv', bucket_name='kumparan', replace=True)


    @task(task_id=f'{source_table}_to_snowflake')
    def load_to_snowflake(s3_path: str, table_name: str, schema_name: str, file_format: str):
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake')
        sql = f"""
        COPY INTO {schema_name}.{table_name}
        FROM '{s3_path}'
        FILE_FORMAT = ({file_format})
        """
        snowflake_hook.run(sql)

    s3_path = 's3://kumparan/pg_data/'
    table_name = 'ARTICLE'
    schema_name = 'PUBLIC'
    file_format = "TYPE = 'CSV', FIELD_DELIMITER = ',' "

    load_from_s3_to_snowflake=load_to_snowflake(s3_path, table_name, schema_name, file_format)

    extract(source_table, dag_conf) >> load_from_s3_to_snowflake

    

    