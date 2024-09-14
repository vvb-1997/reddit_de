import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    'owner': 'Vinay Bhat',
    'start_date': datetime(2024, 9, 3)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

# extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

# upload to s3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

# upload_s3 = S3CreateObjectOperator(
#     task_id="s3_upload",
#     aws_conn_id= 'AWS_CONN',
#     s3_bucket='reddit-de-vvb01',
#     s3_key="raw/{{ti.xcom_pull(task_ids='reddit_extraction', key='return_value').split('/')[-1]}}",
#     data="{{ ti.xcom_pull(task_ids='reddit_extraction', key='return_value') }}",    
# )

extract >> upload_s3
# extract