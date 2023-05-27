import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

dag_path = os.path.join(os.path.dirname(__file__))
dag_name = os.path.join(os.path.basename(__file__)).replace('.py', '')

project_name = 'dwh'
dbt_directory = f'/{dag_path}/../dbt/'
dbt_project = f'/{dbt_directory}/{project_name}'
data_csv_directory = f'/{dag_path}/../data_csv/'

# RUN
run_command = f"\
    dbt run \
    --profiles-dir {dbt_directory} \
    --project-dir {dbt_project} \
    --target prod_airflow"
run_command += " --vars '{ target_date: {{ds}} }' "


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
    dag_id='dbt_model',
    start_date=datetime(2022, 9, 10),
    end_date=datetime(2022, 9, 12),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=True,
    max_active_runs=1
) as dag:
          
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # @task(task_id="copy_dbt_target_files_to_s3")
    # def copy_csv_files_to_s3():
    #     s3_hook = S3Hook()
        
    #     s3_hook.load_file(filename =f'{data_csv_directory}/customer/customer_{{ ds }}.csv',
    #                       bucket_name='raw',
    #                       key='customer/customer_{{ ds }}.csv',
    #                       replace=True)
        
    #     s3_hook.load_file(filename =f'{data_csv_directory}/customerDriver/customerDriver_{{ ds }}.csv',
    #                       bucket_name='raw',
    #                       key='customerDriver/customerDriver_{{ ds }}.csv',
    #                       replace=True)
        
    #     s3_hook.load_file(filename =f'{data_csv_directory}/transactions/loanTrx_{{ ds }}.csv',
    #                       bucket_name='raw',
    #                       key='transactions/loanTrx_{{ ds }}.csv',
    #                       replace=True)
    
    # To get the connection id, go to your connection in airbyte
    # http://localhost:56174/workspaces/e46c12b3-10ad-4711-be3d-ba57ed4c0a15/connections/24886e0c-8467-404d-aaca-dabc888dcd15/status 
    # The string value after connection and before status is the connection_id
    # source_loan_transactions = AirbyteTriggerSyncOperator(
    #     task_id="airbyte_async_S3_to_ods.loanTrx",
    #     connection_id='24886e0c-8467-404d-aaca-dabc888dcd15',
    #     asynchronous=True,
    # )

    # airbyte_sensor_loan_transactions = AirbyteJobSensor(
    #     task_id="airbyte_sensor_S3_to_ods.loanTrx",
    #     airbyte_job_id=source_loan_transactions.output,
    # )
            
    run_dbt_bronze_models = BashOperator(
        task_id='run_dbt_bronze_models',
        bash_command=run_command + "--select bronze"
    )
    
    run_dbt_silver_models = BashOperator(
        task_id='run_dbt_silver_models',
        bash_command=run_command + "--select silver"
    )
    
    run_dbt_gold_models = BashOperator(
        task_id='run_dbt_gold_models',
        bash_command=run_command + "--select gold"
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
    
    
    start >> run_dbt_bronze_models >> run_dbt_silver_models >> run_dbt_gold_models >> run_dbt_tests_models >> copy_dbt_target_files_to_s3() >> end