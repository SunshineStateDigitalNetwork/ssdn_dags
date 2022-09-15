# SSDN DAG template

from datetime import datetime, timedelta
import sys
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.models import Variable

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import ssdn_assets

with DAG('ssdn_harvest_test',
         default_args={'depends_on_past': False,
                       'email': ['airflow.example.org'],
                       'email_on_failure': False,
                       'email_on_retry': False,
                       'retries': 1,
                       'retry_delay': timedelta(minutes=5),
                       },
         description='Template DAG for SSDN',
         tags=['ssdn', 'harvest', 'test'],
         start_date=datetime(2022, 6, 1),
         schedule_interval='@quarterly',
         ) as dag:

    @task
    def print_env_var():
        v = Variable.get("ssdn_env")
        print(v)

    print_env_var = print_env_var()

    #@task
    #def list_print():
    #    l = Variable.get("ssdn_list", deserialize_json=True)
    #    for i in l:
    #        print(i)

    #list_print = list_print()

    harvest_mdpl = BashOperator(
        task_id='harvest_mdpl',
        env={"MANATUS_CONFIG": Variable.get('ssdn_env')},
        bash_command='python3 -m manatus --profile ssdn harvest -s mdpl'
    )

    print_env_var >> harvest_mdpl
