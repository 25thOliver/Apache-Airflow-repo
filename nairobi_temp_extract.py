from airflow.decorators import dag, task
from datetime import datetime
import requests
import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow DAG default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
}

@dag(
    dag_id="nairobi_temp_daily",
    schedule_interval="0 0 * * *",  # run daily at midnight
    start_date=datetime(2025, 9, 6),
    catchup=False,
    default_args=default_args,
    tags=["nairobi", "weather"]
)
def nairobi_temperature_etl():

    @task()
    def extract_temp():
        url = "https://api.open-meteo.com/v1/forecast?latitude=-1.2921&longitude=36.8219&hourly=temperature_2m"
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        raw_file = "/home/oliver/Downloads/nairobi_hourly_raw.json"
        with open(raw_file, "w") as f:
            json.dump(data, f)

        return raw_file

    @task()
    def transform_temp(raw_file_path):
        with open(raw_file_path, "r") as f:
            data = json.load(f)

        times = data["hourly"]["time"]
        temps = data["hourly"]["temperature_2m"]

        df = pd.DataFrame({
            "timestamp": times,
            "temperature": temps
        })

        csv_path = "/home/oliver/Downloads/nairobi_hourly_raw.csv"
        df.to_csv(csv_path, index=False)

        print(f"Transformed data saved to {csv_path}")
        return csv_path

    @task()
    def load_temp(csv_file_path):
        hook = PostgresHook(postgres_conn_id="my_postgres_conn")  
        conn = hook.get_conn()
        cur = conn.cursor()

        # Create the table if it doesn’t exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nairobi_temperature (
                timestamp TIMESTAMP PRIMARY KEY,
                temperature FLOAT
            );
        """)

        # Truncate or append depending on your needs
        cur.execute("TRUNCATE TABLE nairobi_temperature;")

        # Use Postgres COPY for fast CSV load
        with open(csv_file_path, "r") as f:
            next(f)  # skip header
            cur.copy_from(f, "nairobi_temperature", sep=",")
        
        conn.commit()
        cur.close()
        conn.close()

        print("Data loaded efficiently into PostgreSQL")


    raw_file = extract_temp()
    csv_file = transform_temp(raw_file)
    load_temp(csv_file)

# Instantiate the DAG
nairobi_temperature_etl()
