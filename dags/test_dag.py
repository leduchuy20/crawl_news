from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_stack_ok",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test = BashOperator(
        task_id="echo_test",
        bash_command="echo AIRFLOW OK"
    )
