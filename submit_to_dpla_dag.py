"""
## Submit data to DPLA DAG

Trigger with a config: `{"file": "<data submission file>"}`
    Airflow system variables:
        - ssdn_env
        - gdrive_credential_path or gdrive_credential_token
        - gdrive_log_folder
"""

from datetime import datetime, timedelta
import sys
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.models import Variable

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
         tags=['ssdn', 'submit'],
         start_date=datetime(2023, 1, 1),
         schedule_interval='@quarterly',
         catchup=False,
         doc_md=__doc__,
         ) as dag:

    s3_upload = BashOperator(
        task_id='s3_upload',
        bash_command='aws s3 cp {{ dag_run.conf["file"] }} s3://dpla-hub-fl/',
    )

    check_s3_contents = BashOperator(
        task_id='check_s3_contents',
        bash_command='aws s3 ls s3://dpla-hub-fl',
    )

    @task(task_id='publish_reports')
    def publish_reports():
        """

        :return:
        """
        manatus_tsv_log_fp = os.path.join(ssdn_assets.LOG_PATH, 'manatus_errors.tsv')
        gdrive_folder_id = Variable.get('gdrive_folder_id')
        credentials = ssdn_assets.gdrive_file_auth(Variable.get("gdrive_credential_path"))
        partner_csv_reports = ssdn_assets.spreadsheet_separator(manatus_tsv_log_fp)
        partner_gdrive_reports = ssdn_assets.upload_to_folder(gdrive_folder_id, credentials, partner_csv_reports)
        return partner_gdrive_reports

    publish_reports = publish_reports()

    s3_upload >> check_s3_contents >> publish_reports
