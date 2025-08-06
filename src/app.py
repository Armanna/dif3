#!/usr/bin/env python3
from hippo import reload_files_for_glue
reload_files_for_glue()

import datetime
from hippo.dags.lib.v2 import secrets
import click
import unittest

from tasks import cvs_invoice, cvs_invoice_python, walmart_invoice, walgreens_invoice, cvs_quarterly, cvs_yearly, \
                walgreens_quarterly, walmart_quarterly, walmart_quarterly_new, goodrx_invoice, webmd_invoice, drfirst_invoice, \
                cvs_quarterly_python, walgreens_invoice_python, riteaid_invoice, riteaid_summary, famulus_statement, waltz_statement, \
                rxlink_statement, walgreens_quarterly_python, kroger_and_haris_teeter_invoice, kroger_and_harris_teeter_summary, \
                kroger_and_harris_teeter_claims_reporting, publix_invoice, publix_quarterly, publix_marketplace_summary, caprx_statement, meijer_invoice, \
                meijer_summary, download_claims, download_other_sources, wipe_temp_data_in_s3, pos_invoice, rxpartner_statement, \
                albertsons_invoice, albertsons_reconciliation

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option('--task', help='task to run')
@click.option('--hsdk-env', envvar="HSDK_ENV")
@click.option("--invoice-bucket", envvar="INVOICE_BUCKET")
@click.option("--goodrx-bucket", envvar="GOODRX_BUCKET")
@click.option("--test-run-flag", envvar="TEST_RUN_FLAG", default=None) # enabling this flag guaranty that 
@click.option("--manual-date-string", envvar="MANUAL_DATE_STRING", default=None) 
@click.option("--chain-name", envvar="CHAIN_NAME", default=None) 
@click.option("--period", envvar="PERIOD", default=None) 
# default is #invoice
@click.option("--slack-channel", envvar='SLACK_CHANNEL', default='C0204L4Q1BP')
@click.option("--pos-feed-prefix", envvar='POS_FEED_PREFIX')
@click.option("--goodrx-historical-prefix", envvar='GOODRX_HISTORICAL_PREFIX')
@click.option("--temp-slack-channel", envvar='TEMP_SLACK_CHANNEL', default='C057UP179AB') # for test purpose only 'C057UP179AB' --> #temp-inoices
@click.option("--slack-bot-token", envvar='SLACK_BOT_TOKEN', default=secrets.read_aws_parameter("/terraform/etl-runtime-config/SLACK_BOT_TOKEN"))
# exporters cfg
@click.option("--s3-exporter-enabled", envvar='S3_EXPORTER_ENABLED', default=True, type=click.BOOL)
@click.option("--pharmacy-history-bucket", envvar="PHARMACY_HISTORY_BUCKET")
@click.option("--pharmacy-history-prefix", envvar="PHARMACY_HISTORY_PREFIX")
@click.option("--medispan-bucket", envvar="MEDISPAN_BUCKET")
@click.option("--medispan-prefix", envvar="MEDISPAN_PREFIX")
@click.option("--pbm-hippo-bucket", envvar="PBM_HIPPO_BUCKET")
@click.option("--pbm-hippo-prefix", envvar="PBM_HIPPO_PREFIX")
def main(task, **kwargs):
    if task == 'cvs_invoice':
        cvs_invoice.run(**kwargs)
    elif task == 'run_tests':
        test_loader = unittest.TestLoader()
        test_suite = test_loader.discover('tests', pattern='test_*.py')
        unittest.TextTestRunner().run(test_suite)
    elif task == 'download_claims':
        download_claims.run(**kwargs)
    elif task == 'download_sources':
        download_other_sources.run(**kwargs)
    elif task == 'wipe_temp_data_in_s3':
        wipe_temp_data_in_s3.run(**kwargs)
    elif task == 'cvs_invoice_python':
        cvs_invoice_python.run(**kwargs)
    elif task == 'kroger_bi_weekly':
        kroger_and_haris_teeter_invoice.run(**kwargs)
    elif task == 'harris_teeter_bi_weekly':
        kroger_and_haris_teeter_invoice.run(**kwargs)
    elif task == 'kroger_quarterly':
        kroger_and_harris_teeter_claims_reporting.run(**kwargs)
    elif task == 'harris_teeter_quarterly':
        kroger_and_harris_teeter_claims_reporting.run(**kwargs)
    elif task == 'kroger_yearly_summary':
        kroger_and_harris_teeter_summary.run(**kwargs)
    elif task == 'harris_teeter_yearly_summary':
        kroger_and_harris_teeter_summary.run(**kwargs)
    elif task == 'kroger_yearly_claims_reporting':
        kroger_and_harris_teeter_claims_reporting.run(**kwargs)
    elif task == 'harris_teeter_yearly_claims_reporting':
        kroger_and_harris_teeter_claims_reporting.run(**kwargs)
    elif task == 'meijer_summary':
        meijer_summary.run(**kwargs)
    elif task == 'walmart_invoice':
        walmart_invoice.run(**kwargs)
    elif task == 'meijer_invoice':
        meijer_invoice.run(**kwargs)
    elif task == 'walgreens_invoice':
        walgreens_invoice.run(**kwargs)
    elif task == 'walgreens_invoice_python':
        walgreens_invoice_python.run(**kwargs)
    elif task == 'walgreens_quarterly_python':
        walgreens_quarterly_python.run(**kwargs)
    elif task == 'cvs_quarterly_python':
        cvs_quarterly_python.run(**kwargs)
    elif task == 'cvs_quarterly_famulus':
        cvs_quarterly.run('019876','famulus',**kwargs)
    elif task == 'walgreens_quarterly':
        walgreens_quarterly.run(**kwargs)
    elif task == 'walmart_quarterly_new': # new quarterly logic build based on 2024-09-03 Contract requariments
        walmart_quarterly_new.run(**kwargs)
    elif task == 'cvs_yearly':
        cvs_quarterly_python.run(**kwargs)
    elif task == 'goodrx_invoice':
        goodrx_invoice.run(**kwargs)
    elif task == 'webmd_invoice':
        webmd_invoice.run(**kwargs)
    elif task == 'drfirst_invoice':
        drfirst_invoice.run(**kwargs)
    elif task == 'riteaid_invoice':
        riteaid_invoice.run(**kwargs)
    elif task == 'riteaid_summary':
        riteaid_summary.run(**kwargs)
    elif task == 'famulus_statement':
        famulus_statement.run(**kwargs)
    elif task == 'waltz_statement':
        waltz_statement.run(**kwargs)
    elif task == 'rxlink_statement':
        rxlink_statement.run(**kwargs)
    elif task == 'rxpartner_statement':
        rxpartner_statement.run(**kwargs)
    elif task == 'publix_invoice':
        publix_invoice.run(**kwargs)
    elif task == 'publix_quarterly':
        publix_quarterly.run(**kwargs)
    elif task == 'publix_marketplace_summary':
        publix_marketplace_summary.run(**kwargs)
    elif task == 'caprx_statement':
        caprx_statement.run(**kwargs)
    elif task == 'pos_invoice':
        pos_invoice.run(**kwargs)
    elif task == 'albertsons_invoice':
        albertsons_invoice.run(**kwargs)
    elif task == 'albertsons_reconciliation':
        albertsons_reconciliation.run(**kwargs)
    else:
        ctx = click.get_current_context()
        click.echo(ctx.get_help())


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        if e.code != 0:
            raise
