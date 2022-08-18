from datetime import datetime, timedelta
import sys
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.models import Variable

PATH = os.path.abspath(os.path.dirname(__file__))
SSDN_ENV = Variable.get('ssdn_env')

# Import local module
sys.path.insert(0, PATH)
import ssdn_assets


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

    repo_update = BashOperator(
        task_id='repo_update',
        bash_command=f'bash {PATH}/ssdn_assets/repo_update.sh {Variable.get("ssdn_git_repos")}',
    )

    for partner in ssdn_assets.list_config_keys(ssdn_assets.harvest_parser):
        partner_harvest = BashOperator(
            task_id=f'harvest_{partner}',
            bash_command=f'python3 -m manatus --profile ssdn harvest -s {partner}',
        )

    repo_update.set_downstream(partner_harvest)
