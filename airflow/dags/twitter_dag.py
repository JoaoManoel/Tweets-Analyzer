from datetime import datetime
from os.path import join
from airflow.models import DAG
from operators.twitter_operator import TwitterOperator


with DAG(
    dag_id='twitter_dag',
    start_date=datetime.utcnow()
) as dag:
    twitter_operator = TwitterOperator(
        task_id='twitter_palmeiras',
        query='AvantiPalestra',
        file_path=join(
            '~',
            'data'
            'tweets',
            'extract_date={{ ds }}',
            'AvantiPalestra_{{ ds_nodash }}.jsonl'
        )
    )
