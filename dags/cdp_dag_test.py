from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.decorators import task
from datetime import datetime

default_args = {
    'owner': 'Avinash Newton',
    'email': 'avinash.newton@takeda.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retry': 1,
}

@task(task_id='010-cdp-batch-control-start')
def start_batch_control():
    return DatabricksSubmitRunOperator(
        task_id='010-cdp-batch-control-start',
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        existing_cluster_id=EXISTING_CLUSTER_ID,
        run_name='Batch_Start_Job',
        notebook_task={
            "notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/start_batch_control",
            "base_parameters": {
                "batch_name": BATCH_NAME,
                "user_id": USER_ID,
                "mwaa_region": MWAA_REGION,
                "mwaa_env_name": MWAA_ENV_NAME,
                "mwaa_iam_role": MWAA_IAM_ROLE
            },
            "source": "WORKSPACE"
        },
        do_xcom_push=True
    )

@task(task_id='020-file_1-sheet-1-raw-to-lake')
def file_1_sheet_1_raw_to_lake():
    return DatabricksSubmitRunOperator(
        task_id='020-file_1-sheet-1-raw-to-lake',
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        existing_cluster_id=EXISTING_CLUSTER_ID,
        run_name='020-file_1-sheet-1-raw-to-lake',
        notebook_task={
            "notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/Wrapper",
            "base_parameters": {
                "app_id": APP_ID,
                "batch_name": BATCH_NAME,
                "job_name": "020-file_1-sheet-1-raw-to-lake",
                "entity_id": "020-file_1-sheet-1-raw-to-lake",
                "user_id": USER_ID,
                "sns_topic_arn": SNS_TOPIC_ARN,
                "mwaa_region": MWAA_REGION,
                "mwaa_env_name": MWAA_ENV_NAME,
                "mwaa_iam_role": MWAA_IAM_ROLE
            },
            "source": "WORKSPACE"
        },
        do_xcom_push=True
    )

@task(task_id='030-file_1-sheet-2-raw-to-lake')
def file_1_sheet_2_raw_to_lake():
    return DatabricksSubmitRunOperator(
        task_id='030-file_1-sheet-2-raw-to-lake',
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        existing_cluster_id=EXISTING_CLUSTER_ID,
        run_name='030-file_1-sheet-2-raw-to-lake',
        notebook_task={
            "notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/Wrapper",
            "base_parameters": {
                "app_id": APP_ID,
                "batch_name": BATCH_NAME,
                "job_name": "030-file_1-sheet-2-raw-to-lake",
                "entity_id": "030-file_1-sheet-2-raw-to-lake",
                "user_id": USER_ID,
                "sns_topic_arn": SNS_TOPIC_ARN,
                "mwaa_region": MWAA_REGION,
                "mwaa_env_name": MWAA_ENV_NAME,
                "mwaa_iam_role": MWAA_IAM_ROLE
            },
            "source": "WORKSPACE"
        },
        do_xcom_push=True
    )

@task(task_id='040-file_1-sheet-3-raw-to-lake')
def file_1_sheet_3_raw_to_lake():
    return DatabricksSubmitRunOperator(
        task_id='040-file_1-sheet-3-raw-to-lake',
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        existing_cluster_id=EXISTING_CLUSTER_ID,
        run_name='040-file_1-sheet-3-raw-to-lake',
        notebook_task={
            "notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/Wrapper",
            "base_parameters": {
                "app_id": APP_ID,
                "batch_name": BATCH_NAME,
                "job_name": "040-file_1-sheet-3-raw-to-lake",
                "entity_id": "040-file_1-sheet-3-raw-to-lake",
                "user_id": USER_ID,
                "sns_topic_arn": SNS_TOPIC_ARN,
                "mwaa_region": MWAA_REGION,
                "mwaa_env_name": MWAA_ENV_NAME,
                "mwaa_iam_role": MWAA_IAM_ROLE
            },
            "source": "WORKSPACE"
        },
        do_xcom_push=True
    )

@task(task_id='050-archive-file_1')
def archive_file_1():
    return DatabricksSubmitRunOperator(
        task_id='050-archive-file_1',
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        existing_cluster_id=EXISTING_CLUSTER_ID,
        run_name='050-archive-file_1',
        notebook_task={
            "notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/Wrapper",
            "base_parameters": {
                "app_id": APP_ID,
                "batch_name": BATCH_NAME,
                "job_name": "050-archive-file_1",
                "entity_id": "050-archive-file_1",
                "custom_notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/raw_file_archival",
                "custom_notebook_param": "app_id::cdp_development_mragan,entity_id::050-archive-file_1",
                "user_id": USER_ID,
                "sns_topic_arn": SNS_TOPIC_ARN,
                "mwaa_region": MWAA_REGION,
                "mwaa_env_name": MWAA_ENV_NAME,
                "mwaa_iam_role": MWAA_IAM_ROLE
            },
            "source": "WORKSPACE"
        },
        do_xcom_push=True
    )

@task(task_id='060-cdp-batch-control-stop')
def stop_batch_control():
    return DatabricksSubmitRunOperator(
        task_id='060-cdp-batch-control-stop',
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        existing_cluster_id=EXISTING_CLUSTER_ID,
        run_name='Batch_Stop_Job',
        notebook_task={
            "notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/stop_batch_control",
            "base_parameters": {
                "batch_name": BATCH_NAME,
                "user_id": USER_ID
            },
            "source": "WORKSPACE"
        },
        do_xcom_push=True
    )

with DAG(
    dag_id='cdp-core-mwaa-existing-cluster',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['Takeda', 'CDP', 'Databricks','demo','avinash'],
    default_args=default_args
) as dag:

    # Fetch the variables from Airflow required for the DAG
    MWAA_ENV_NAME = Variable.get("MWAA_ENV_NAME")
    MWAA_IAM_ROLE = Variable.get("MWAA_IAM_ROLE")
    MWAA_REGION = Variable.get("MWAA_REGION")
    SNS_TOPIC_ARN = Variable.get("SNS_TOPIC_ARN")
    EXISTING_CLUSTER_ID = Variable.get("CDP_Personal-ML-Cluster-MRagan-Service")

    # Define the Databricks connection for the DAG
    DATABRICKS_CONNECTION_ID = "ENTCDPServAccount"

    # Define the CDP pipeline variables
    APP_ID = "cdp_development_mragan"
    BATCH_NAME = 'cdp-core-mwaa-integration-existing-cluster-tests'
    USER_ID = 'marian.ragan@takeda.com'

    t1 = start_batch_control()
    t2 = file_1_sheet_1_raw_to_lake() 
    t3 = file_1_sheet_2_raw_to_lake()
    t4 = file_1_sheet_3_raw_to_lake()
    t5 = archive_file_1()
    t6 = stop_batch_control()

    # Set the task dependencies
    t1 >> [t2, t3, t4] >> t5 >> t6    