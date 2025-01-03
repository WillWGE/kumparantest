from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime,timedelta
import os
import logging
import pytz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'catchup': False,
}

with DAG('without_s3_pipeline',
            catchup=False,
            default_args=default_args,
            description='A DAG to load data from PostgreSQL to Snowflake, with created_at update',
            schedule_interval="0 * * * *",  # Schedule to run at the top of every hour
        ) as dag:

    # Function to extract data from PostgreSQL
    def extract_from_postgres():
        postgres_hook = PostgresHook(postgres_conn_id='airflow_kumparan')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        # Define the file path
        file_path = 'C:/Users/hp/Documents/kumparan/include/data.csv'
        
        # Ensure the directory exists
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        
         # Get the current time in Jakarta timezone
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        current_time_jakarta = datetime.now(jakarta_tz)
        # Update created_at column with the current timestamp
        update_sql = """
        UPDATE kumparan.article
        SET created_at = %s
        WHERE created_at IS NULL
        """
        cursor.execute(update_sql, (current_time_jakarta,))
        connection.commit()
        
        # SQL command to copy data to CSV without header
        sql = "COPY (SELECT * FROM kumparan.article ORDER BY id) TO STDOUT WITH CSV DELIMITER ','"
        
        # Execute the copy command and save to file
        with open(file_path, 'w') as file:
            cursor.copy_expert(sql, file)
        
        cursor.close()
        connection.close()
    
    # Function to load data into Snowflake
    def load_into_snowflake():
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake')
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        # Upload the CSV file to Snowflake internal stage
        cursor.execute(f"REMOVE @~/stage/data.csv;")
        cursor.execute("PUT file://C:/Users/hp/Documents/kumparan/include/data.csv @~/stage/data.csv;")
        
        # Truncate the target table before loading new data
        cursor.execute("TRUNCATE TABLE public.article;")

        # Load data from the stage into the Snowflake table
        cursor.execute("""
            COPY INTO public.article
            FROM @~/stage/data.csv
            FILE_FORMAT = (type = 'CSV' field_optionally_enclosed_by = '\"' skip_header=0)
        """)
        
        cursor.close()
        conn.close()


    

    # Define the tasks
    extract_task = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_from_postgres,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id='load_into_snowflake',
        python_callable=load_into_snowflake,
        dag=dag,
    )


    extract_task >> load_task  

