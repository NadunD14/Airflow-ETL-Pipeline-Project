from airflow import DAG
from airflow.operators.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.dates import days_ago
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
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_conn')

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


    ##step 3: Transform the dat(Pick the information that I need to save)


    ##step 4: Load the data into the Postgres table


    ##steep 5: Verify the data DBViewer


    ##step 6: Define the task dependencies