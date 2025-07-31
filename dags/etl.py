from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json

# Define the DAG
with DAG(
    dag_id='nasa_apod_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    # Step 1: Create the table if it does not exist
    @task
    def create_table():
        # Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        # SQL query to create the table (fixed table name and removed trailing comma)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        # Execute the table creation query
        postgres_hook.run(create_table_query)

    # Step 2: Extract the NASA API data 
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},
        response_filter=lambda response: response.json(),
        log_response=True
    )

    # Step 3: Transform the data (Pick the information that I need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            "title": response.get('title', ''),
            "explanation": response.get('explanation', ''),
            "url": response.get('url', ''),
            "date": response.get('date', ''),
            "media_type": response.get('media_type', '')
        }
        return apod_data

    # Step 4: Load the data into the Postgres table
    @task
    def load_data_into_postgres(apod_data):
        # Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        
        # SQL query to insert data into the table (fixed table name)
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        # Execute the insert query with the transformed data
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    # Step 5: Define the task dependencies
    # Create table first
    create_table_task = create_table()
    
    # Set up the pipeline
    create_table_task >> extract_apod
    
    # Transform the extracted data
    transformed_data = transform_apod_data(extract_apod.output)
    
    # Load the transformed data
    load_data_into_postgres(transformed_data)