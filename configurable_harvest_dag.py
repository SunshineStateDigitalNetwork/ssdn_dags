from datetime import datetime, timedelta
import sys
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain

PATH = os.path.abspath(os.path.dirname(__file__))
SSDN_ENV = Variable.get('ssdn_env')

# Import local module
sys.path.insert(0, PATH)
import ssdn_assets


with DAG('ssdn_single_harvest',
         default_args={'depends_on_past': False,
                       'email': ['airflow.example.org'],
                       'email_on_failure': False,
                       'email_on_retry': False,
                       'retries': 1,
                       'retry_delay': timedelta(minutes=5),
                       'env': {'MANATUS_CONFIG': SSDN_ENV}
                       },
         description='Single partner harvest',
         tags=['ssdn', 'harvest', ],
         start_date=datetime(2022, 1, 1),
         schedule_interval='@quarterly',
         catchup=False,
         ) as dag:

    repo_update = BashOperator(
        task_id='repo_update',
        bash_command=f'bash {PATH}/ssdn_assets/repo_update.sh {Variable.get("ssdn_git_repos")}',
    )

    s3_upload = BashOperator(
        task_id='s3_upload',
        bash_command='whoami',
    )

    clean_up = BashOperator(
        task_id='clean_up',
        env={'DATA_PATH': ssdn_assets.OAI_PATH},
        bash_command='rm -rf $DATA_PATH/{{ dag_run.conf["partner"] }}/*.xml',
    )

    partner_harvest = BashOperator(
        task_id=f'single_harvest',
        bash_command='python3 -m manatus --profile ssdn harvest -s {{ dag_run.conf["partner"] }}',
    )

    chain([repo_update, clean_up], partner_harvest, [s3_upload])
