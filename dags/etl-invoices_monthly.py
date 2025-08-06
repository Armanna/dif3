import os
from datetime import datetime, timedelta
from airflow import DAG
from lib.operators.glue import HippoGlueJobOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'retry_delay': timedelta(minutes=60),
}

AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
AWS_REGION = os.getenv("AWS_REGION")

INVOICE_BUCKET = 'hippo-invoices-{}-{}'.format(AWS_ACCOUNT_ID, AWS_REGION)
MEDISPAN_BUCKET='etl-medispan-files-{}-{}'.format(AWS_ACCOUNT_ID, AWS_REGION)

ENV = {
    'INVOICE_BUCKET': INVOICE_BUCKET,
    'HSDK_ENV': utils.get_hippo_env(),
    'MANUAL_DATE_STRING': "{{ dag_run.conf.get('MANUAL_DATE_STRING') }}", # by default: None
    'TEST_RUN_FLAG': "{{ dag_run.conf.get('TEST_RUN_FLAG') }}", # by default: "False"
    'MEDISPAN_BUCKET': MEDISPAN_BUCKET,
    'MEDISPAN_PREFIX': utils.get_last_date(MEDISPAN_BUCKET, 'export'),
}

dag = DAG(
    'etl-invoices_monthly',
    default_args=default_args,
    schedule_interval='13 6 1 * *',
    max_active_runs=1,
    catchup=False,
)

dag.doc_md = __doc__

kwargs = {
    'repository_name': 'etl-invoices',
    'execution_timeout': timedelta(minutes=90)
}

start = DummyOperator(task_id='start',
                      dag=dag
                      )

generate_cvs_invoice = HippoGlueJobOperator(
    task_id='cvs_invoice',
    script_args={**ENV, **{'TASK': 'cvs_invoice'}},
    dag=dag,
    **{**kwargs, 'worker_type': 'G.4X'}
)

generate_cvs_invoice.doc_md = """\
Generate CVS mothly invoice
"""

generate_goodrx_invoice = HippoGlueJobOperator(
    task_id='goodrx_invoice',
    script_args={**ENV, **{'TASK': 'goodrx_invoice'}},
    dag=dag,
    **kwargs
)

generate_goodrx_invoice.doc_md = """\
How much money do we owe GoodRx for last month's fills?
"""

generate_drfirst_invoice = HippoGlueJobOperator(
    task_id='drfirst_invoice',
    script_args={**ENV, **{'TASK': 'drfirst_invoice'}},
    dag=dag,
    **kwargs
)

generate_drfirst_invoice.doc_md = """\
Margin share with Dr first for RXinform and Rcopia
"""

generate_walgreens_invoice = HippoGlueJobOperator(
    task_id='walgreens_invoice',
    script_args={**ENV, **{'TASK': 'walgreens_invoice'}},
    dag=dag,
    **kwargs
)

generate_walgreens_invoice.doc_md = """\
Generate invoice and data for Walgreens. Post to slack in #invoices
"""

generate_riteaid_invoice = HippoGlueJobOperator(
    task_id='riteaid_invoice',
    script_args={**ENV, **{'TASK': 'riteaid_invoice'}},
    dag=dag,
    **kwargs
)

generate_riteaid_invoice.doc_md = """\
Generate invoice and data for Rite Aid. Post to slack in #invoices
"""

generate_riteaid_summary = HippoGlueJobOperator(
    task_id='riteaid_summary',
    script_args={**ENV, **{'TASK': 'riteaid_summary', 'period': 'month'}},
    dag=dag,
    **kwargs
)

generate_riteaid_summary.doc_md = """\
#Generate quarterly Utilization file and GER data for Rite Aid. Post to slack in #invoices
"""

generate_publix_invoice = HippoGlueJobOperator(
    task_id='publix_invoice',
    script_args={**ENV, **{'TASK': 'publix_invoice'}},
    dag=dag,
    **kwargs
)

publix_marketplace_summary = HippoGlueJobOperator(
    task_id='publix_marketplace_summary_monthly',
    script_args={**ENV, **{'period': 'month', 'TASK': 'publix_marketplace_summary'}},
    dag=dag,
    **kwargs
)

publix_marketplace_summary.doc_md = """\
#Generate monthly claims reporting file for Publix Marketplace contract. Post to slack in #invoices

"""


generate_publix_invoice.doc_md = """\
Generate invoice and data for Publix. Post to slack in #invoices
"""

generate_webmd_invoice = HippoGlueJobOperator(
    task_id='webmd_invoice',
    script_args={**ENV, **{'TASK': 'webmd_invoice'}},
    dag=dag,
    **kwargs
)

generate_webmd_invoice.doc_md = """\
Generate invoice and data for WebMd. Post to slack in #invoices
"""

generate_famulus_statement = HippoGlueJobOperator(
    task_id='famulus_statement',
    script_args={**ENV, **{'TASK': 'famulus_statement'}},
    dag=dag,
    **kwargs
)

generate_famulus_statement.doc_md = """\
Generate statement for Famulus. Post to slack in #invoices
"""

generate_waltz_statement = HippoGlueJobOperator(
    task_id='waltz_statement',
    script_args={**ENV, **{'TASK': 'waltz_statement'}},
    dag=dag,
    **kwargs
)

generate_waltz_statement.doc_md = """\
Generate statement for Waltz. Post to slack in #invoices
"""

generate_rxlink_statement = HippoGlueJobOperator(
    task_id='rxlink_statement',
    script_args={**ENV, **{'TASK': 'rxlink_statement'}},
    dag=dag,
    **kwargs
)

generate_rxlink_statement.doc_md = """\
Generate statement for RxLink. Post to slack in #invoices
"""

run_tests = HippoGlueJobOperator(
    task_id='run_tests',
    script_args={**ENV, **{'TASK': 'run_tests'}},
    dag=dag,
    **kwargs
)

run_tests.doc_md = """\
#Run tests before start main task
"""

generate_walgreens_invoice_python = HippoGlueJobOperator(
    task_id='walgreens_invoice_python',
    script_args={**ENV, **{'TASK': 'walgreens_invoice_python'}},
    dag=dag,
    **kwargs
)

generate_walgreens_invoice_python.doc_md = """\
Generate invoice and data for Walgreens (python). Post to slack in #temp-invoices
"""

generate_caprx_statement = HippoGlueJobOperator(
    task_id='caprx_statement',
    script_args={**ENV, **{'TASK': 'caprx_statement'}},
    dag=dag,
    **kwargs
)

generate_caprx_statement.doc_md = """\
Generate statement for Caprx. Post to slack in #invoices
"""

generate_meijer_invoice = HippoGlueJobOperator(
    task_id='meijer_invoice',
    script_args={**ENV, **{'TASK': 'meijer_invoice'}},
    dag=dag,
    **kwargs
)

generate_meijer_invoice.doc_md = """\
Generate invoice for Meijer. Post to slack in #invoices
"""

generate_rxpartner_statement = HippoGlueJobOperator(
    task_id='rxpartner_statement',
    script_args={**ENV, **{'TASK': 'rxpartner_statement'}},
    dag=dag,
    **kwargs
)

generate_rxpartner_statement.doc_md = """\
Generate statement and transaaction files for RxPartner. Post to slack in #invoices
"""



start >> generate_cvs_invoice
start >> generate_goodrx_invoice
start >> generate_walgreens_invoice
start >> generate_drfirst_invoice
start >> generate_riteaid_invoice
start >> generate_riteaid_summary
start >> generate_famulus_statement
start >> generate_webmd_invoice
start >> generate_rxlink_statement
start >> generate_publix_invoice
start >> generate_caprx_statement
start >> generate_meijer_invoice
start >> generate_rxpartner_statement

start >> run_tests
start >> generate_walgreens_invoice_python
