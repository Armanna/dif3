# ETL pipeline for invoice generation

**Owner**: Andrew Stead `<stead@hellohippo.com>`

# Overview

This etl creates invoices, backs them up to S3 and then sends to the slack #invoice channel.

You should not put PII data in the invoices or utilization files. There should be no member ID going to slack.

# Testing

Slack token available from here https://api.slack.com/apps/A0207MRPT0S/general? . Click on OAuth & Permissions.

Make sure you are on the vpn and have an ssh tunnel to redshift

```bash
jumpbox 5439 hippo-redshift-cluster.co56vh5uqlw5.us-east-1.redshift.amazonaws.com 5439
```

To set your environment do the following

```bash
$(hsdk se d)
. ./local.sh
pip3 install -r requirements-dev.txt
# get the latest version of hippo lib from etl-lib and substitute below
pip3 install /Users/stead/dev/etl-lib/libs/py3-etl/dist/hippo-0.14.7-py2.py3-none-any.whl
```

Update to use localhost redshift. Either change

[./src/sources/redshift.py](./src/sources/redshift.py)

and add the following

```python
self.downloader = RedshiftDownloader(localhost_flag=True)
```

or add the following to `/etc/hosts/`

```bash
127.0.0.1       hippo-redshift-cluster.co56vh5uqlw5.us-east-1.redshift.amazonaws.com
```

Run

```bash
./app.py --task=cvs_invoice
./app.py --task=walmart_invoice 
./app.py --task=walgreens_invoice 
./app.py --task=cvs_quarterly
./app.py --task=cvs_quarterly_tpdt
./app.py --task=cvs_quarterly_webmd
./app.py --task=cvs_yearly 
./app.py --task=walgreens_quarterly 
./app.py --task=walmart_quarterly 
./app.py --task=goodrx_invoice 
./app.py --task=webmd_invoice
./app.py --task=drfirst_invoice 
./app.py --task=riteaid_invoice 
./app.py --task=famulus_statement 
./app.py --task=waltz_statement 
```

## Unit tests
```bash
python3 ./tests/walmart_quarterly_test.py 
python3 ./tests/cvs_pdf_gen_test.py
```

# Re-running prior invoices

Sometimes it's necessary to re-generate a previous invoice e.g. in case of bug fix. The easiest way to do this (for now) is to hard-code the dates in the sql and then run manually.

Dates are inclusive of start and end date. Here is an example you can use

```sql
with date_range as (
    select
        '20210416'::date as period_start,
        '20210430'::date as period_end
),
```

# Invoices

## CVS

The CVS contract is here: https://hellohippo.slack.com/files/U7C0CCZBR/F01T60R5NNB/cw2407603_-_hippo_pharmacy_services_agreement_2021v4.docx_-_signed.pdf

This contains details of what's expected in the invoice and when it should be delivered. Currently no mention of how. We're sending directly to Charles contact ( Patrick.Walsh2@cvshealth.com ).

The sql queries were developed using the following mode report. This can be used to manually generate the utilization data and invoice summary information if required

https://app.mode.com/editor/hippo/reports/3b0e151c774a

### CVS Quarterly

Once a quarter we need to send over data to CVS to validate adherance to the GER / BER targets. From the CVS contract

```
On a quarterly basis, PBM shall calculate the GER and BER for the prior calendar quarter and provide a GER and BER calculation report to CVS by no later than thirty (30) days following the end of the first, second, and third calendar quarter (“Quarterly Report”). The Quarterly Report will identify any underpayment and will provide reasonably sufficient information to support the calculations contained in the report. At a minimum, the Quarterly Report shall include (i) the beginning and end dates for the reporting period; (ii) total number of applicable claims; (iii) total AWP for applicable claims; (iv) total ingredient cost paid for applicable claims; (v) the actual GER and BER; (vi) the contracted GER and BER; (vii) the GER and/or BER variance, if any; and (viii) total dollar amount of the GER and/or BER variance, if any.
```



## Walmart

The Walmart contract is here: https://hellohippo.slack.com/files/U7C0CCZBR/F01VC0ECFEY/hip-amd_first_amendment_-wm_draft-_01.06.2021.docx.pdf

This contains details of what's expected in the invoice and when it should be delivered.

These files should be attached to an email and sent to (according to their time basis which is 15th of the month for Monthly):

HWBillingRecon <hwbilli@wal-mart.com>,
Alex Talashkevich <alext@hellohippo.com>,
Laura Jackson <laura@hellohippo.com>,
Charles Jacoby <charles@hellohippo.com>,
Bryan Craycraft <Donald.Craycraft@walmart.com>,
Angela Hamil <Angela.Hamil@walmart.com>

The following text can be used

Hi,

Please find attached the Hippo invoice and utilization data for MONTH.

Please let me know if you have any questions.

Thanks,

Your NAME

The sql queries were developed using the following mode report. This can be used to manually generate the utilization data and invoice summary information if required

https://app.mode.com/hippo/reports/b732061db324

## Walgreens

The Walgreens contract is here: https://hellohippo.slack.com/files/U7C0CCZBR/F01ULHLPTCN/master_psa__hippo_04162021_v2__mob__elr_cln.pdf

This contains details of what's expected in the invoice and when it should be delivered. Currently no mention of how. Need to confirm with Charles

### Walgreens Quarterly

Once a quarter we need to send over data to Walgreens to validate adherance to the GER / BER targets. From the Walgreens contract

```
Within 10 business days of the end of each calendar quarter or the termination date of this Agreement, Client will provide Walgreens with a summary report and Claim-Level Detail, Discount Card Network Claim Level Detail and Discount Card Network Administration Fee Claim Level Detail for all claims for Covered Drugs dispensed during that calendar quarter, excluding any reversed claims.
```

After speaking with Walgreens they have indicated that `Discount Card Network Claim Level Detail` is `Exhibit D PRESCRIPTION CLAIMS DATA DETAIL`.  This also covers `Claim-Level Detail`.

That leaves the following three files to produce once per quarter:

- Summary report
- Claim level detail report
- Discount Card Network Administration Fee Claim Level Detail

The last one is defined in the contract

```
“Discount Card Network Administration Fee Claim Level Detail" means the NCPDP number, NPI, prescription number, date filled, prescription type (Brand Name Drug /Generic Drug/Reversal), store number, contracted rate, PARTICIPANT pay amount and Discount Card Administration fee for each Discount Card Network Covered Drug included in the invoice.
```

## GoodRx

The GoodRx contract is here: https://www.dropbox.com/home/01%20Hippo%20Dataroom%20General/05%20Partnerships/01%20Prospects/GoodRx/Contracts

This is complicated by a rider that allows us to keep half the savings we make with any of our PBM partners. The rider is also in the same folder in Dropbox

## Rite Aid

The Rite Aid contract is here: https://hellohippo.slack.com/files/U67JP2E4T/F047YGASJ0K/rite_aid_discount_card_agreement_hippo_10.03.2022_sxs_clean_10142022.pdf

Hippo must sent invoice and utilization files on monthly basis together with Annual GER performance report and Annual Claims Report within forty-five (45) calendar days after the end of each calendar year.
