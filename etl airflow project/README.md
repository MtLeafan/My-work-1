# ETL Pipeline with Apache Airflow

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Apache Airflow.

## Steps
1. Extract data from a CSV file.
2. Transform data (filter employees from the IT department).
3. Load data into an SQLite database.

## How to Run
1. Install Apache Airflow.
2. Place the `etl_pipeline.py` file in the `dags` folder.
3. Place the `employees.csv` file in the `data` folder.
4. Run Airflow and enable the DAG.