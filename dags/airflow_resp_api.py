from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import requests
import base64
import os

def list_dag_runs():
    mwaa = boto3.client('mwaa')
    env_name = 'MWAA1USVGA92123D004'
    dag_name = 'snow_demo_dag_v1'

    token_response = mwaa.create_cli_token(Name=env_name)
    endpoint = token_response['WebServerHostname']
    jwt_token = token_response['CliToken']

    mwaa_cli_endpoint = f'https://{endpoint}/aws_mwaa/cli'
    command = f'dags list-runs -d {dag_name}'

    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Content-Type': 'text/plain'
    }

    mwaa_response = requests.post(mwaa_cli_endpoint, headers=headers, data=command)
    if mwaa_response.status_code == 200:
        result = base64.b64decode(mwaa_response.json()['stdout']).decode('utf-8')
        print(result)
    else:
        raise Exception(f"MWAA CLI call failed: {mwaa_response.text}")

with DAG(
    dag_id='mwaa_list_dag_runs',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['mwaa', 'cli'],
) as dag:

    list_runs = PythonOperator(
        task_id='list_dag_runs',
        python_callable=list_dag_runs
    )

    list_runs

