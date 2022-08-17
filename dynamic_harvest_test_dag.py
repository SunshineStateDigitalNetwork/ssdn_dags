from datetime import datetime, timedelta
import sys
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.models import Variable

PATH = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PATH)
import ssdn_assets

SSDN_ENV = Variable.get('ssdn_env')

with DAG('ssdn_dynamic_harvest',
         default_args={'depends_on_past': False,
                       'email': ['airflow.example.org'],
                       'email_on_failure': False,
                       'email_on_retry': False,
                       'retries': 1,
                       'retry_delay': timedelta(minutes=5),
                       'env': {'MANATUS_CONFIG': SSDN_ENV}
                       },
         description='Dynamic harvest test',
         tags=['ssdn', 'harvest', 'dynamic', 'test'],
         start_date=datetime(2045, 1, 1),
         ) as dag:

    # PATH = Path(__file__)
    # sys.path.append(str(PATH.absolute()))
    # import ssdn_assets

    # @task
    # def print_env_var():
    #     v = Variable.get("ssdn_env")
    #     print(v)
    #
    # print_env_var = print_env_var()

    # @task
    # def list_print():
    #     l = Variable.get("ssdn_git_repos", deserialize_json=True)
    #     for i in l:
    #         print(i)
    #
    # list_print = list_print()

    # harvest_mdpl = BashOperator(
    #     task_id='harvest_mdpl',
    #     env={"MANATUS_CONFIG": Variable.get('ssdn_env')},
    #     bash_command='python3 -m manatus --profile ssdn harvest -s mdpl'
    # )
    #
    # print_env_var >> list_print >> harvest_mdpl

    repo_update = BashOperator(
        task_id='repo_update',
        bash_command=f'bash {PATH}/ssdn_assets/repo_update.sh {Variable.get("ssdn_git_repos")}',
    )

    for partner in ssdn_assets.list_config_keys(ssdn_assets.harvest_parser):
        partner_harvest = BashOperator(
            task_id=f'harvest_{partner}',
            bash_command='python3 -m manatus --profile ssdn harvest -s {partner}',
        )

    partner_harvest.set_upstream(repo_update)

