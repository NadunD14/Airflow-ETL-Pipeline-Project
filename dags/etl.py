from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

##Define the DAG
with DAG(
    dag_id='nasa_apod_etl',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False

) as dag:
    
    ## step 1: Create the table if it does not exist

    @task
    def create_table():
        ## initialize the postgressHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        ## SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apo_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50),
        );
        """

        ## Execute the table creation query
        postgres_hook.run(create_table_query)

    ## step 2: Extract the NASA API data 
    ##https://api.nasa.gov/planetary/apod?api_key=odgPIFqyw9oWjW2wWRYLGwwvmuIBxiqASg5ZkPUQ
    
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod?api_key=DEMO_KEY',
        method='GET',
        data={"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response: response.json()
    )

    ##step 3: Transform the dat(Pick the information that I need to save)

    @task
    def transform_apod_data(response):
        apod_data={
            "title": response.get('title', ''),
            "explanation": response.get('explanation', ''),
            "url": response.get('url', ''),
            "date": response.get('date', ''),
            "media_type": response.get('media_type', '')
        }
        return apod_data

    ##step 4: Load the data into the Postgres table

    @task
    def load_data_into_postgres(apod_data):
        ## initialize the postgressHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        
        ## SQL query to insert data into the table
        insert_query = """
        INSERT INTO apo_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute the insert query with the transformed data
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    ##steep 5: Verify the data DBViewer


    ##step 6: Define the task dependencies
    ##Extratct
    create_table() >> extract_apod ##Ensure the table is created before extracting data
    api_response = extract_apod.output
    ##Transform
    transformed_data = transform_apod_data(api_response)
    ##Load
    load_data_into_postgres(transformed_data) ##Ensure the data is loaded into the table after
