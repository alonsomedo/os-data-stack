import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from cosmos.providers.dbt.task_group import DbtTaskGroup

dag_path = os.path.join(os.path.dirname(__file__))
dag_name = os.path.join(os.path.basename(__file__)).replace('.py', '')

project_name = 'dwh'
dbt_directory = f'/{dag_path}/../dbt/'
dbt_project = f'/{dbt_directory}/{project_name}'

test_command = f"\
    dbt test \
    --profiles-dir {dbt_directory} \
    --project-dir {dbt_project} \
    --target prod_airflow"


default_args = {
    'owner': 'alonso_md',
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': True
}

with DAG(
    dag_id='run_dbt_model_cosmos',
    description="Postgres connection needs to be configured to run this pipeline",
    start_date=datetime(2022, 9, 10),
    end_date=datetime(2022, 9, 12),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=True,
    max_active_runs=1
) as dag:
          
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    dbt_tg = DbtTaskGroup(
        group_id="dbt_dwh_project",
        dbt_project_name="dwh",
        conn_id="postgres_default",
        dbt_root_path=dbt_directory,
        dbt_args={
            "schema": 'dwh',
            "vars": '{"target_date": "{{ ds }}"}'
        }
    )
    
    run_dbt_tests_models = BashOperator(
        task_id='run_dbt_tests_models',
        bash_command=test_command
    )
    
    @task(task_id="copy_dbt_target_files_to_s3")
    def copy_dbt_target_files_to_s3():
        s3_hook = S3Hook()
        s3_hook.load_file(filename =f'{dbt_project}/target/manifest.json',
                          bucket_name='dbt',
                          key='dwh/target/manifest.json',
                          replace=True)
        
        s3_hook.load_file(filename =f'{dbt_project}/target/run_results.json',
                          bucket_name='dbt',
                          key='dwh/target/run_results.json',
                          replace=True)
    
    
    start >> dbt_tg >> run_dbt_tests_models >> copy_dbt_target_files_to_s3() >> end