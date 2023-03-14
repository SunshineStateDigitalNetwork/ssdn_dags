"""
## Submit data to DPLA DAG

Trigger with a config: `{"file": "<data submission file>"}`
"""

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

with DAG('submit_to_dpla',
         default_args={'depends_on_past': False,
                       'email': ['airflow.example.org'],
                       'email_on_failure': False,
                       'email_on_retry': False,
                       'retries': 1,
                       'retry_delay': timedelta(minutes=5),
                       },
         description='Submit finished data to DPLA',
         tags=['ssdn',],
         start_date=datetime(2045, 1, 1),
         ) as dag:

    s3_upload = BashOperator(
        task_id='s3_upload',
        bash_command='aws s3 ls s3://dpla-hub-fl && echo {{ dag_run.conf["file"] }}',
    )

    print_working_dir = BashOperator(
        task_id='pwd',
        bash_command='pwd',
    )

    print_working_dir >> s3_upload
