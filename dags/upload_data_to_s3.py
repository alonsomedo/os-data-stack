import os 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


dag_path = os.path.join(os.path.dirname(__file__))
dag_name = os.path.join(os.path.basename(__file__)).replace('.py', '')
data_csv_directory = f'/{dag_path}/../data_csv'


default_args = {
    'owner': 'alonso_md',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id=dag_name,
    start_date=datetime(2022, 9, 10),
    end_date=datetime(2022, 9, 12),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=True,
    max_active_runs=1
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    @task(task_id="copy_csv_files_to_s3")
    def copy_csv_files_to_s3(**context):
        
        s3_hook = S3Hook()
        
        s3_hook.load_file(filename =f'{data_csv_directory}/customer/customer_{context["ds"]}.csv',
                          bucket_name='raw',
                          key=f'customer/customer_{context["ds"]}.csv',
                          replace=True)
        
        s3_hook.load_file(filename =f'{data_csv_directory}/customerDrivers/customerDrivers_{context["ds"]}.csv',
                          bucket_name='raw',
                          key=f'customerDrivers/customerDrivers_{context["ds"]}.csv',
                          replace=True)
        
        s3_hook.load_file(filename =f'{data_csv_directory}/transactions/loanTrx_{context["ds"]}.csv',
                          bucket_name='raw',
                          key=f'transactions/loanTrx_{context["ds"]}.csv',
                          replace=True)
        

    start >> copy_csv_files_to_s3() >> end