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
    'TEST_RUN_FLAG': "{{ dag_run.conf.get('TEST_RUN_FLAG') }}", # by default: "False"
    'MANUAL_DATE_STRING': "{{ dag_run.conf.get('MANUAL_DATE_STRING') }}", # by default: None
    'HSDK_ENV': utils.get_hippo_env(),
}

dag = DAG(
    'etl-invoices_walmart-quarterly',
    default_args=default_args,
    schedule_interval='21 8 15 1,4,7,10 *',
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = """
    If you want to run the task in test mode i.e. when it's too big for local run - start DAG with w/ сonfig: {"TEST_RUN_FLAG": "True"}. 
    It will ensure that the messages will be sent to the #temp-invoices Slack channel instead of #invoices and files will be saved under /temp/ prefix in S3 bucket
    If you want to run the report for specific date you need to add to the config: {'MANUAL_DATE_STRING': '2025-01-01'}.
    """

kwargs = {
    'repository_name': 'etl-invoices',
    'execution_timeout': timedelta(minutes=60),
    'worker_type': 'G.2X'
}

download_claims = HippoGlueJobOperator(
    task_id='download_claims',
    script_args={**ENV, **{'chain_name': 'walmart', 'period': 'quarter', 'TASK': 'download_claims'}},
    dag=dag,
    **kwargs
)
download_claims.doc_md = """
# Download claims for Walmart quarterly report. Saves resulting dataframe as .parquet file

"""

download_sources = HippoGlueJobOperator(
    task_id='download_sources',
    script_args={**ENV, **{'chain_name': 'walmart', 'period': 'quarter', 'TASK': 'download_sources'}},
    dag=dag,
    **kwargs
)
download_sources.doc_md = """
# Download shource files for Walmart quarterly report. Saves resulting dataframe as .parquet file

"""

walmart_quarterly_new = HippoGlueJobOperator(
    task_id='walmart_quarterly_new',
    script_args={**ENV, **{'chain_name': 'walmart', 'period': 'quarter', 'TASK': 'walmart_quarterly_new'}},
    dag=dag,
    **kwargs
)

walmart_quarterly_new.doc_md = """\
#Generate quarterly invoice and data for Walmart based on the new contract (September 3 2024). Post to slack in #invoices
If you want to run task for test purpose i.e. when it's too big for local run - start DAG with w/ сonfig: {"TEST_RUN_FLAG": "True"}. 
It will ensure that the messages will be sent to the #temp-invoices Slack channel instead of #invoices and files will be saved under /temp/ prefix in S3 bucket
"""

wipe_temp_data_in_s3 = HippoGlueJobOperator(
    task_id=f'wipe_temp_data_in_s3_all_data',
    script_args={**ENV, **{'chain_name': 'walmart', 'period': 'quarter', 'TASK': 'wipe_temp_data_in_s3'}},
    dag=dag,
    **kwargs
)

wipe_temp_data_in_s3.doc_md = """\
# Delete all temporal data in s3://hippo-invoices-... /temp/sources/ and temp/claims/ path for the specific chain only. 
"""

download_claims >> download_sources >> walmart_quarterly_new >> wipe_temp_data_in_s3
