import pytz
import pendulum
import datetime

from airflow import DAG
# from docker.types import Mount
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.operators.docker import DockerOperator
import subprocess

local_tz = pendulum.timezone("Asia/Jakarta")
idn = pytz.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_fatlure': False,
    'email_on_retry': False,
    'start_date': datetime.datetime(2021, 12, 2, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'params': {
        'priority': 'P0',
        'current_timezone_start_datetime': datetime.datetime.now(idn).strftime("%m/%d/%Y, %H:%M:%S")
    }
}


def run_elt_script():
    script_path = "/opt/airflow/elt/elt_script.py"
    result = subprocess. run(["python", script_path],
                             capture_output=True, text=True)
    if result. returncode != 0:
        raise Exception(f"Script failed with error: {result.stderr}")
    else:
        print(result.stdout)


dag = DAG('elt_ghaly',
          tags=['Kumparan', 'Test', 'Ghaly'],
          schedule_interval='0 * * * *',
          default_args=default_args,
          description='Postgresql to PostgreSQL',
          #   start_date=datetime(2016, 07, 08),
          catchup=False
          )

start_task = DummyOperator(
    task_id="start_task",
    dag=dag
)

t1 = PythonOperator(
    task_id="run_elt_script",
    python_callable=run_elt_script,
    dag=dag
)

end_task = DummyOperator(task_id='end_task', dag=dag)


start_task >> t1 >> end_task
