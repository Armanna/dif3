#!/bin/bash

export AWS_ACCOUNT_ID=`echo ${HSDK_ENV_ID} | awk '{split($1,a,"-");print(a[1])}'`
export AWS_REGION=${AWS_DEFAULT_REGION}
export INVOICE_BUCKET=hippo-invoices-${HSDK_ENV_ID}
export PHARMACY_HISTORY_BUCKET=etl-pharmacy-${HSDK_ENV_ID}
export PHARMACY_HISTORY_PREFIX=export/history/
export MEDISPAN_BUCKET=etl-medispan-files-${HSDK_ENV_ID}
export MEDISPAN_PREFIX="export/`aws s3 ls s3://${MEDISPAN_BUCKET}/export/ | tail -n 1 | awk '{print $2}' | sed 's/\///g'`"
export HSDK_ENV=dev # export HSDK_ENV='production' to run in production
export PBM_HIPPO_BUCKET=etl-pbm-hippo-${HSDK_ENV_ID}
export PBM_HIPPO_PREFIX=/exports/history/
export SLACK_CHANNEL=C057UP179AB # test channel #temp-invoices ONLY FOR TESTS
export TEMP_SLACK_CHANNEL=C057UP179AB # test channel #temp-invoices ONLY FOR TESTS
export TEST_RUN_FLAG='False'
export MANUAL_DATE_STRING=None
export CHAIN_NAME='cvs'
export PERIOD='month'
