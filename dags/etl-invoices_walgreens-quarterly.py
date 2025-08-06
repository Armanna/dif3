import os
from datetime import datetime, timedelta
from airflow import DAG
from lib.operators.glue import HippoGlueJobOperator
from lib.v2 import utils, secrets

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 31, 12, 00, 00),
    'email': ['stead@hellohippo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'concurrency': 1,
    'retries': 1,
    'on_failure_callback': utils.send_slack_message,
    'on_retry_callback': utils.send_slack_message,
    'retry_delay': timedelta(minutes=30),
}

AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
AWS_REGION = os.getenv("AWS_REGION")

INVOICE_BUCKET = 'hippo-invoices-{}-{}'.format(AWS_ACCOUNT_ID, AWS_REGION)

ENV = {
    'INVOICE_BUCKET': INVOICE_BUCKET,
    'HSDK_ENV': utils.get_hippo_env(),
    'MANUAL_DATE_STRING': "{{ dag_run.conf.get('MANUAL_DATE_STRING') }}", # by default: None
    'TEST_RUN_FLAG': "{{ dag_run.conf.get('TEST_RUN_FLAG') }}", # by default: "False"
}

dag = DAG(
    'etl-invoices_walgreens-quarterly',
    default_args=default_args,
    schedule_interval='21 8 16 */3 *',
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

kwargs = {
    'repository_name': 'etl-invoices',
    'execution_timeout': timedelta(minutes=60)
}

walgreens_quarterly = HippoGlueJobOperator(
    task_id='walgreens_quarterly',
    script_args={**ENV, **{'TASK': 'walgreens_quarterly'}},
    dag=dag,
    **kwargs
)

walgreens_quarterly.doc_md = """\
#Generate quarterly invoice and data for Walgreen BIN 019876. Post to slack in #invoices

"""

walgreens_quarterly_python = HippoGlueJobOperator(
    task_id='walgreens_quarterly_python',
    script_args={**ENV, **{'TASK': 'walgreens_quarterly_python'}},
    dag=dag,
    **kwargs
)

walgreens_quarterly_python.doc_md = """\
# Generate quarterly summary (python-based) and data for Walgreens. Post to slack in #temp-invoices

"""
