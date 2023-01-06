""" STOCK STREAMING DAG """
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.main import main


# schedule 10am-4pm Monday-Friday
stream_workflow = DAG(
    dag_id="StockStreaming",
    description="Stream real-time stock data",
    schedule_interval="*/5 10-16 * * 1-5",
    catchup=False
)

with stream_workflow:
    producer = PythonOperator(
        task_id='producer',
        retries=2,
        retry_delay=timedelta(seconds=10),
        python_callable=main,
        op_kwards=dict(task='producer')
    )

    consumer = PythonOperator(
        task_id='consumer',
        retries=2,
        retry_delay=timedelta(seconds=10),
        python_callable=main,
        op_kwards=dict(task='consumer')
    )

    producer >> consumer
