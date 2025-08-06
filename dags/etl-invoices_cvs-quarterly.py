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
MEDISPAN_BUCKET='etl-medispan-files-{}-{}'.format(AWS_ACCOUNT_ID, AWS_REGION)
PBM_HIPPO_BUCKET='etl-pbm-hippo-{}-{}'.format(AWS_ACCOUNT_ID, AWS_REGION)
PBM_HIPPO_PREFIX='/exports/history/'


ENV = {
    'INVOICE_BUCKET': INVOICE_BUCKET,
    'MEDISPAN_BUCKET': MEDISPAN_BUCKET,
    'MEDISPAN_PREFIX': utils.get_last_date(MEDISPAN_BUCKET, 'export'),
    'HSDK_ENV': utils.get_hippo_env(),
    'MANUAL_DATE_STRING': "{{ dag_run.conf.get('MANUAL_DATE_STRING') }}", # by default: None
    'TEST_RUN_FLAG': "{{ dag_run.conf.get('TEST_RUN_FLAG') }}", # by default: "False"
    'PBM_HIPPO_BUCKET': PBM_HIPPO_BUCKET,
    'PBM_HIPPO_PREFIX': PBM_HIPPO_PREFIX,
}

dag = DAG(
    'etl-invoices_cvs-quarterly',
    default_args=default_args,
    schedule_interval='21 9 16 */3 *',
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
    'execution_timeout': timedelta(minutes=90),
    'worker_type': 'G.8X'
}

run_tests = HippoGlueJobOperator(
    task_id='run_tests',
    script_args={**ENV, **{'TASK': 'run_tests'}},
    dag=dag,
    **kwargs
)

run_tests.doc_md = """\
#Run tests before start main task
"""

download_claims = HippoGlueJobOperator(
    task_id='download_claims',
    script_args={**ENV, **{'chain_name': 'cvs', 'period': 'quarter', 'TASK': 'download_claims'}},
    dag=dag,
    **kwargs
)
download_claims.doc_md = """
# Download claims for CVS quarterly report. Saves resulting dataframe as .parquet file

"""

download_sources = HippoGlueJobOperator(
    task_id='download_sources',
    script_args={**ENV, **{'chain_name': 'cvs', 'period': 'quarter', 'TASK': 'download_sources'}},
    dag=dag,
    **kwargs
)
download_sources.doc_md = """
# Download shource files for CVS quarterly report. Saves resulting dataframe as .parquet file

"""

cvs_quarterly_python = HippoGlueJobOperator(
    task_id='cvs_quarterly_python',
    script_args={**ENV, **{'chain_name': 'cvs', 'period': 'quarter', 'TASK': 'cvs_quarterly_python'}},
    dag=dag,
    **kwargs
)

cvs_quarterly_python.doc_md = """\
#Generate quarterly reconciliation for all CVS programs. Post to slack in #invoices

"""

wipe_temp_data_in_s3 = HippoGlueJobOperator(
    task_id=f'wipe_temp_data_in_s3_all_data',
    script_args={**ENV, **{'chain_name': 'cvs', 'period': 'quarter', 'TASK': 'wipe_temp_data_in_s3'}},
    dag=dag,
    **kwargs
)

wipe_temp_data_in_s3.doc_md = """\
# Delete all temporal data in s3://hippo-invoices-... /temp/sources/ and temp/claims/ path. 
"""

download_claims >> download_sources >> cvs_quarterly_python >> wipe_temp_data_in_s3
