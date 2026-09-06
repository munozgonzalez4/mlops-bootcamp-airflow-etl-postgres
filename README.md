# mlops-bootcamp-airflow-etl-postgres

Pre-requisite: install Astro CLI
PowerShell: winget install -e --id Astronomer.Astro

Initialize repo:
astro dev init

Docker will be used to run Airflow and Postgres as services

WE also use Airflow hooks and operators to handle efficiently the ETL process 

dags/etl_pipeline.py will contain the dag and tasks

We need to build the connections in the Airflow UI to make this work