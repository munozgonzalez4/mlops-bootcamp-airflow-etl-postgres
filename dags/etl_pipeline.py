from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
import json


# Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    # Step 1: Create the table if it doesn't exist
    @task
    def create_table():

        # Initialize the Postgres hook
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        # SQL statement to create the table
        create_table_sql = """
        SELECT pg_advisory_xact_lock(hashtext('nasa_apod_schema'));

        CREATE TABLE IF NOT EXISTS nasa_apod (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        # Execute the SQL statement
        pg_hook.run(create_table_sql)

    # Step 2: extract data from the NASA APOD API
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', # Connection ID for the NASA API defined in Airflow Connections
        endpoint='planetary/apod',
        method='GET',
        data={'api_key': '{{conn.nasa_api.extra_dejson.api_key}}'}, # Use the API key from the connection's extra field
        extra_options={'timeout': 30},
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        response_filter=lambda response: response.json(), # Parse the response as JSON 
    )

    # Step 3: transform the data (pick the information that needs to be saved)
    @task
    def transform_apod(apod_data):
        # Extract relevant fields from the API response
        transformed_data = {
            'title': apod_data.get('title', ''),
            'explanation': apod_data.get('explanation', ''),
            'url': apod_data.get('url', ''),
            'date': apod_data.get('date', ''),
            'media_type': apod_data.get('media_type', '')
        }
        return transformed_data

    # Step 4: load the data into the Postgres database
    @task
    def load_apod_to_postgres(transformed_data):
        # Initialize the Postgres hook
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        # SQL statement to insert the data into the table
        insert_sql = """
        INSERT INTO nasa_apod (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        # Execute the SQL statement with the transformed data
        pg_hook.run(insert_sql, parameters=(
            transformed_data['title'],
            transformed_data['explanation'],
            transformed_data['url'],
            transformed_data['date'],
            transformed_data['media_type']
        ))

    # Step 5: define the task dependencies
    create_table() >> extract_apod
    api_response = extract_apod.output
    transformed_data = transform_apod(api_response)
    load_apod_to_postgres(transformed_data)