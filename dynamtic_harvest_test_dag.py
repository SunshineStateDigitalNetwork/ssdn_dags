from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.models import Variable

PATH = Path(__file__)
sys.path.append(str(PATH))
import ssdn_assets


with DAG('ssdn_dynamic_harvet',
         default_args={'depends_on_past': False,
                       'email': ['airflow.example.org'],
                       'email_on_failure': False,
                       'email_on_retry': False,
                       'retries': 1,
                       'retry_delay': timedelta(minutes=5),
                       },
         description='Dynamic harvest test',
         tags=['ssdn', 'harvest', 'dynamic', 'test'],
         start_date=datetime(2045, 1, 1),
         ) as dag:

    # @task
    # def print_env_var():
    #     v = Variable.get("ssdn_env")
    #     print(v)
    #
    # print_env_var = print_env_var()
    #
    # @task
    # def list_print():
    #     l = Variable.get("ssdn_list", deserialize_json=True)
    #     for i in l:
    #         print(i)
    #
    # list_print = list_print()
    #
    # harvest_mdpl = BashOperator(
    #     task_id='harvest_mdpl',
    #     env={"MANATUS_CONFIG": Variable.get('ssdn_env')},
    #     bash_command='python3 -m manatus --profile ssdn harvest -s mdpl'
    # )
    #
    # print_env_var >> list_print >> harvest_mdpl

    for partner in ssdn_assets.list_config_keys(ssdn_assets.harvest_parser):
        partner_harvest = BashOperator(
            task_id=f'harvest_{partner}',
            # do some bash stuff
        )
