import os
from datetime import datetime, timedelta
from airflow import DAG
from lib.operators.glue import HippoGlueJobOperator
from lib.v2 import utils, secrets
from airflow.utils.log.secrets_masker import mask_secret
from airflow.sensors.s3_key_sensor import S3KeySensor


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
GOODRX_BUCKET = "etl-partner-goodrx-files-{}-{}".format(AWS_ACCOUNT_ID, AWS_REGION)
MEDISPAN_BUCKET='etl-medispan-files-{}-{}'.format(AWS_ACCOUNT_ID, AWS_REGION)
EXECUTION_DATE = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y.%m.%d") }}'

ENV = {
    'INVOICE_BUCKET': INVOICE_BUCKET,
    'GOODRX_BUCKET': GOODRX_BUCKET,
    'POS_FEED_PREFIX': f'exports/buydown_files',
    'GOODRX_HISTORICAL_PREFIX': f'exports/history',
    'TEST_RUN_FLAG': "{{ dag_run.conf.get('TEST_RUN_FLAG') }}", # by default: "False"
    'MANUAL_DATE_STRING': "{{ dag_run.conf.get('MANUAL_DATE_STRING') }}", # by default: None
    'HSDK_ENV': utils.get_hippo_env(),
    'MEDISPAN_BUCKET': MEDISPAN_BUCKET,
    'MEDISPAN_PREFIX': utils.get_last_date(MEDISPAN_BUCKET, 'export'),
}

mask_secret("REDSHIFT_PASSRORD")


dag = DAG(
    'etl-invoices_bi-weekly',
    default_args=default_args,
    schedule_interval='0 10 1,16 * *',
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
    'execution_timeout': timedelta(minutes=60)
}

download_claims = HippoGlueJobOperator(
    task_id='download_claims',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period': 'bi-weekly', 'TASK': 'download_claims'}},
    dag=dag,
    **kwargs
)
download_claims.doc_md = """
# Download claims for Albertsons invoice. Saves resulting dataframe as .parquet file

"""

download_sources = HippoGlueJobOperator(
    task_id='download_sources',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period': 'bi-weekly', 'TASK': 'download_sources'}},
    dag=dag,
    **kwargs
)
download_sources.doc_md = """
# Download shource files for Albertsons invoice. Saves resulting dataframe as .parquet file

"""

generate_albertsons_invoice = HippoGlueJobOperator(
    task_id='albertsons_invoice',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period':'bi-weekly', 'TASK': 'albertsons_invoice'}},
    dag=dag,
    **kwargs
)

generate_albertsons_invoice.doc_md = """\
#Generate invoice and data for Albertsons. Post to slack in #invoices

"""

generate_kroger_invoice = HippoGlueJobOperator(
    task_id='kroger_bi_weekly',
    script_args={**ENV, **{'chain_name': 'kroger', 'TASK': 'kroger_bi_weekly'}},
    dag=dag,
    **kwargs
)

generate_kroger_invoice.doc_md = """\
#Generate invoice and data for Kroger. Post to slack in #invoices

"""

generate_harris_teeter_invoice = HippoGlueJobOperator(
    task_id='harris_teeter_bi_weekly',
    script_args={**ENV, **{'chain_name': 'harris_teeter', 'TASK': 'harris_teeter_bi_weekly'}},
    dag=dag,
    **kwargs
)

generate_harris_teeter_invoice.doc_md = """\
#Generate invoice and data for Harris Teeter. Post to slack in #invoices

"""

yesterday_execution_date_pos_file = '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}'

pos_invoice_sensor = S3KeySensor(
    task_id='pos_invoice_sensor',
    bucket_name=GOODRX_BUCKET,
    bucket_key=f"{ENV['POS_FEED_PREFIX']}/{EXECUTION_DATE}/dcaw_brand_claims_feed_{yesterday_execution_date_pos_file}.csv",
    timeout=1 * 60 * 60,  # Timeout in seconds (1 hour in this case)
    poke_interval=300,  # Checking interval in seconds
    mode='reschedule',
    dag=dag,
)

generate_pos_invoice = HippoGlueJobOperator(
    task_id='pos_invoice',
    script_args={**ENV, **{'TASK': 'pos_invoice'}},
    dag=dag,
    **kwargs
)

wipe_temp_data_in_s3 = HippoGlueJobOperator(
    task_id=f'wipe_temp_data_in_s3_all_data',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period': 'bi-weekly', 'TASK': 'wipe_temp_data_in_s3'}},
    dag=dag,
    **kwargs
)

wipe_temp_data_in_s3.doc_md = """\
# Delete all temporal data in s3://hippo-invoices-... /temp/sources/ and temp/claims/ path. 
"""

pos_invoice_sensor >> generate_pos_invoice
download_claims >> download_sources >> generate_albertsons_invoice >> wipe_temp_data_in_s3
