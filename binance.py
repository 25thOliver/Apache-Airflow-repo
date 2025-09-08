from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests
import pandas as pd

api = 'https://api.binance.com/api/v3/ticker/24hr'

def fetch_load():
	# Fetch
	response = requests.get(api)
	data = response.json()

	# Transform
	df = pd.DataFrame(data)
	print(df.head())

with DAG(
	dag_id = 'fetch_load',
	start_date = datetime(2025,9,4),
	schedule_interval = '*/5 * * * *',
	catchup = False	
)as dag:

	fetch = PythonOperator(
		task_id = 'fetch_load',
		python_callable = fetch_load	
	)
	fetch
