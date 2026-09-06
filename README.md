# Airflow ETL with PostgreSQL

An educational Airflow example that extracts NASA's Astronomy Picture of the Day
(APOD), transforms the response, and stores it in PostgreSQL. It is designed as a
small reference project for revisiting Airflow DAGs, operators, TaskFlow tasks,
connections, and local-to-cloud deployment.

## What This Example Teaches

The DAG in `dags/etl_pipeline.py` follows this flow:

1. `create_table` uses `PostgresHook` to create the destination table.
2. `HttpOperator` calls the NASA APOD API.
3. `transform_apod` keeps the fields needed by the database.
4. `load_apod_to_postgres` inserts the transformed record.

The project also demonstrates Airflow 3 conventions: `schedule` instead of
`schedule_interval`, a fixed timezone-aware `start_date`, and provider packages
declared in `requirements.txt`.

## Run Locally

### Prerequisites

- Docker Desktop
- Astro CLI

Install Astro CLI on Windows with:

```powershell
winget install -e --id Astronomer.Astro
```

Start the local Airflow environment from the repository root:

```powershell
astro dev start
```

Open the Airflow UI at `http://localhost:8080`.

### Configure Airflow Connections

Create these connections in **Admin > Connections**:

#### `nasa_api`

- Connection type: `HTTP`
- Host: `https://api.nasa.gov`
- Extra:

```json
{"api_key": "YOUR_NASA_API_KEY"}
```

#### `my_postgres_conn`

Point this connection to the PostgreSQL service you want to use. For a local
database, use the PostgreSQL container hostname rather than `localhost` when
Airflow runs inside Docker. Set the host, database, username, password, and port
(`5432`) according to your environment.

Enable the `nasa_apod_postgres` DAG, trigger it manually, and inspect the task
logs in the Airflow UI. The resulting table is `nasa_apod`.

Stop the local environment when finished:

```powershell
astro dev stop
```

## Useful Project Files

- `dags/etl_pipeline.py`: DAG definition and ETL logic
- `requirements.txt`: Airflow provider dependencies
- `Dockerfile`: Astro Runtime image used to build the Airflow image
- `airflow_settings.yaml`: optional local connections, variables, and pools
- `docker-compose.yml`: local PostgreSQL service configuration

## Deploy to the Cloud with Astro and AWS RDS

Astro hosts the Airflow deployment; AWS RDS hosts PostgreSQL. These are two
separate pieces that must be able to communicate over the network.

1. Create a PostgreSQL database in AWS RDS. Record its endpoint, port, database
	name, username, and password.
2. Configure the RDS security group to allow inbound PostgreSQL traffic from
	the Astro deployment's network. Do not open port `5432` to the whole internet.
3. In the Astro UI, create or select an Astro Deployment on AWS. Confirm that
	its region and networking setup can reach the RDS instance.
4. Authenticate the CLI and deploy this project:

	```powershell
	astro login
	astro workspace switch
	astro deployment list
	astro deployment switch
	astro deploy
	```

5. In the cloud Airflow UI, create the same `nasa_api` connection and update
	`my_postgres_conn` with the RDS endpoint and credentials. Do not commit API
	keys or database passwords to the repository.
6. Enable and trigger the DAG, then verify the task logs and the `nasa_apod`
	table in RDS.

The local `docker-compose.yml` database is for development only. In the cloud,
the DAG should use the RDS connection instead of the local PostgreSQL service.