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
    'etl-invoices_yearly',
    default_args=default_args,
    schedule_interval='54 8 16 1 *',
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
    'worker_type': 'G.8X'
}

rite_aid_yearly = HippoGlueJobOperator(
    task_id='riteaid_yearly',
    script_args={**ENV, **{'TASK': 'riteaid_summary', 'period': 'year'}},
    dag=dag,
    **kwargs
)

rite_aid_yearly.doc_md = """\
#Generate yearly Utilization file and GER data for Rite Aid. Post to slack in #invoices

"""

kroger_yearly_summary = HippoGlueJobOperator(
    task_id='kroger_yearly_summary',
    script_args={**ENV, **{'chain_name': 'kroger', 'period_flag': 'year', 'TASK': 'kroger_yearly_summary'}},
    dag=dag,
    **kwargs
)

kroger_yearly_summary.doc_md = """\
#Generate yearly summary file for Kroger. Post to slack in #invoices

"""

harris_teeter_yearly_summary = HippoGlueJobOperator(
    task_id='harris_teeter_yearly_summary',
    script_args={**ENV, **{'chain_name': 'harris_teeter', 'period_flag': 'year', 'TASK': 'harris_teeter_yearly_summary'}},
    dag=dag,
    **kwargs
)

harris_teeter_yearly_summary.doc_md = """\
#Generate yearly summary file for Harris Teeter. Post to slack in #invoices

"""

kroger_yearly_claims_reporting = HippoGlueJobOperator(
    task_id='kroger_yearly_claims_reporting',
    script_args={**ENV, **{'chain_name': 'kroger', 'period_flag': 'year', 'TASK': 'kroger_yearly_claims_reporting'}},
    dag=dag,
    **kwargs
)

kroger_yearly_claims_reporting.doc_md = """\
#Generate yearly claims reporting file for Kroger. Post to slack in #invoices

"""

harris_teeter_yearly_claims_reporting = HippoGlueJobOperator(
    task_id='harris_teeter_yearly_claims_reporting',
    script_args={**ENV, **{'chain_name': 'harris_teeter', 'period_flag': 'year', 'TASK': 'harris_teeter_yearly_claims_reporting'}},
    dag=dag,
    **kwargs
)

harris_teeter_yearly_claims_reporting.doc_md = """\
#Generate yearly claims reporting file for Harris Teeter. Post to slack in #invoices

"""

meijer_quarterly = HippoGlueJobOperator(
    task_id='meijer_yearly',
    script_args={**ENV, **{'period': 'year', 'TASK': 'meijer_summary'}},
    dag=dag,
    **kwargs
)

meijer_quarterly.doc_md = """\
#Generate yearly claims reporting file for Meijer. Post to slack in #invoices

"""

publix_marketplace = HippoGlueJobOperator(
    task_id='publix_marketplace_yearly',
    script_args={**ENV, **{'period': 'year', 'TASK': 'publix_marketplace_summary'}},
    dag=dag,
    **kwargs
)

publix_marketplace.doc_md = """\
#Generate yearly claims reporting file for Publix Marketplace contract. Post to slack in #invoices

"""

