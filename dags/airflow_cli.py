from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import requests
import base64

def list_dag_runs():
    role_arn = 'arn:aws:iam::263789222982:role/Airflow-Admin'
    session_name = 'airflow-cli-session'
    airflow_env_name = 'MWAA1USVGA92123D004'
    dag_name = 'snow_demo_dag_v1'
    
    aws_api_key = "AKIAIOSFODNN7EXAMPLE"

    # Step 1: Assume IAM Role
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )

    credentials = assumed_role['Credentials']

    # Step 2: Create MWAA client using assumed role credentials
    mwaa_client = boto3.client(
        'mwaa',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
#test2
    # Step 3: Generate MWAA CLI token
    token_response = mwaa_client.create_cli_token(Name=airflow_env_name)
    endpoint = token_response['WebServerHostname']
    jwt_token = token_response['CliToken']

    # Step 4: Build MWAA CLI request
    mwaa_cli_endpoint = f'https://{endpoint}/aws_mwaa/cli'
    command = f'dags list-runs -d {dag_name}'

    

    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Content-Type': 'text/plain'
    }

    mwaa_response = requests.post(mwaa_cli_endpoint, headers=headers, data=command)

    if mwaa_response.status_code == 200:
        result = base64.b64decode(mwaa_response.json()['stdout']).decode('utf-8')
        print("MWAA CLI Output:\n", result)
    else:
        raise Exception(f"MWAA CLI call failed: {mwaa_response.text}")

# Airflow DAG definition
with DAG(
    dag_id='mwaa_list_dag_runs_with_assume_role',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['mwaa', 'assume-role'],
) as dag:

    list_runs = PythonOperator(
        task_id='list_dag_runs',
        python_callable=list_dag_runs
    )


    list_runs



