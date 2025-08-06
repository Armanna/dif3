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
    'MANUAL_DATE_STRING': "{{ dag_run.conf.get('MANUAL_DATE_STRING') }}", # by default: None
    'TEST_RUN_FLAG': "{{ dag_run.conf.get('TEST_RUN_FLAG') }}", # by default: "False"
    'HSDK_ENV': utils.get_hippo_env(),
    'PBM_HIPPO_BUCKET': PBM_HIPPO_BUCKET,
    'PBM_HIPPO_PREFIX': PBM_HIPPO_PREFIX,
}

dag = DAG(
    'etl-invoices_quarterly',
    default_args=default_args,
    schedule_interval='15 9 16 */3 *',
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = """
    If you want to run the report for specific date you need to add to the config: {'MANUAL_DATE_STRING': '2025-01-01'}.
    This is currently supported for Meijer report. 
    """


kwargs = {
    'repository_name': 'etl-invoices',
    'execution_timeout': timedelta(minutes=60)
}

download_claims = HippoGlueJobOperator(
    task_id='download_claims',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period': 'quarter', 'TASK': 'download_claims'}},
    dag=dag,
    **kwargs
)
download_claims.doc_md = """
# Download claims for CVS quarterly report. Saves resulting dataframe as .parquet file

"""

download_sources = HippoGlueJobOperator(
    task_id='download_sources',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period': 'quarter', 'TASK': 'download_sources'}},
    dag=dag,
    **kwargs
)
download_sources.doc_md = """
# Download shource files for CVS quarterly report. Saves resulting dataframe as .parquet file

"""

kroger_quarterly = HippoGlueJobOperator(
    task_id='kroger_quarterly',
    script_args={**ENV, **{'chain_name': 'kroger', 'period': 'quarter', 'TASK': 'kroger_quarterly'}},
    dag=dag,
    **kwargs
)

kroger_quarterly.doc_md = """\
#Generate Kroger quarterly data. Post to slack in #invoices

"""

harris_teeter_quarterly = HippoGlueJobOperator(
    task_id='harris_teeter_quarterly',
    script_args={**ENV, **{'chain_name': 'harris_teeter', 'period': 'quarter', 'TASK': 'harris_teeter_quarterly'}},
    dag=dag,
    **kwargs
)

harris_teeter_quarterly.doc_md = """\
#Generate Harris Teeter quarterly data. Post to slack in #invoices

"""

publix_quarterly = HippoGlueJobOperator(
    task_id='publix_quarterly',
    script_args={**ENV, **{'TASK': 'publix_quarterly'}},
    dag=dag,
    **kwargs
)

publix_quarterly.doc_md = """\
#Generate Publix quarterly data. Post to slack in #invoices

"""

publix_marketplace_quarterly = HippoGlueJobOperator(
    task_id='publix_marketplace_quarterly',
    script_args={**ENV, **{'period': 'quarter', 'TASK': 'publix_marketplace_summary'}},
    dag=dag,
    **kwargs
)

publix_marketplace_quarterly.doc_md = """\
#Generate quarterly claims reporting file for Publix Marketplace contract. Post to slack in #invoices

"""

meijer_quarterly = HippoGlueJobOperator(
    task_id='meijer_quarterly',
    script_args={**ENV, **{'period': 'quarter', 'TASK': 'meijer_summary'}},
    dag=dag,
    **kwargs
)

meijer_quarterly.doc_md = """\
#Generate Meijer quarterly data. Post to slack in #invoices

"""

rite_aid_quarterly = HippoGlueJobOperator(
    task_id='riteaid_quarterly',
    script_args={**ENV, **{'TASK': 'riteaid_summary', 'period': 'quarter'}},
    dag=dag,
    **kwargs
)

rite_aid_quarterly.doc_md = """\
#Generate quarterly Utilization file and GER data for Rite Aid. Post to slack in #invoices
"""

albertsons_quarterly_python = HippoGlueJobOperator(
    task_id='albertsons_reconciliation',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period': 'quarter', 'TASK': 'albertsons_reconciliation'}},
    dag=dag,
    **kwargs
)

albertsons_quarterly_python.doc_md = """\
#Generate quarterly reconciliation for Albertsons. Post to slack in #invoices

"""

wipe_temp_data_in_s3 = HippoGlueJobOperator(
    task_id=f'wipe_temp_data_in_s3_all_data',
    script_args={**ENV, **{'chain_name': 'albertsons', 'period': 'quarter', 'TASK': 'wipe_temp_data_in_s3'}},
    dag=dag,
    **kwargs
)

wipe_temp_data_in_s3.doc_md = """\
# Delete all temporal data in s3://hippo-invoices-... /temp/sources/ and temp/claims/ path. 
"""

download_claims >> download_sources >> albertsons_quarterly_python >> wipe_temp_data_in_s3
