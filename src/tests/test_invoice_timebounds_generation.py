import unittest
import os
import sys
from datetime import datetime
from hippo import logger

lib_path = os.path.abspath('.')
sys.path.append(lib_path)

from transform.utils import process_invoice_dates

log = logger.getLogger('process_invoice_dates_test')

class TestDates(unittest.TestCase):
    def setUp(self):
        log.info('Set up vairables')
        self.period_flag = None
        self.manual_date = None
        
    def test_1_biweekly(self):
        log.info('TEST 1 Started! Bi-weekly (< day 15) period_start & period end same year')
        self.period_flag = 'bi-weekly'
        self.manual_date = '2023-08-02'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2023-07-16", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2023-07-31", "%Y-%m-%d"))

    def test_2_biweekly(self):
        log.info('TEST 2.0 Started! Bi-weekly (< day 15) period_start & period end cross year')
        self.period_flag = 'bi-weekly'
        self.manual_date = '2024-01-01'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2023-12-16", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2023-12-31", "%Y-%m-%d"))

    def test_3_biweekly(self):
        log.info('TEST 2.1 Started! Bi-weekly (> day 15) period_start & period end cross year')
        self.period_flag = 'bi-weekly'
        self.manual_date = '2024-01-16'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2024-01-01", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2024-01-15", "%Y-%m-%d"))

    def test_4_biweekly(self):
        log.info('TEST 2.2 Started! Bi-weekly (day = 15) period_start & period end cross year')
        self.period_flag = 'bi-weekly'
        self.manual_date = '2024-01-15'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2023-12-16", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2023-12-31", "%Y-%m-%d"))

    def test_1_monthly(self):
        log.info('TEST 3 Started! Monthly period_start & period end same year')
        self.period_flag = 'month'
        self.manual_date = '2023-08-02'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2023-07-01", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2023-07-31", "%Y-%m-%d"))

    def test_2_monthly(self):
        log.info('TEST 4 Started! Monthly period_start & period end cross year')
        self.period_flag = 'month'
        self.manual_date = '2024-01-01'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2023-12-01", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2023-12-31", "%Y-%m-%d"))

    def test_1_quarterly(self):
        log.info('TEST 5 Started! Quarterly period_start & period end same year')
        self.period_flag = 'quarter'
        self.manual_date = '2023-08-02'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2023-04-01", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2023-06-30", "%Y-%m-%d"))

    def test_2_quarterly(self):
        log.info('TEST 6 Started! Quarterly period_start & period end cross year')
        self.period_flag = 'quarter'
        self.manual_date = '2024-01-01'
        period_start, period_end = process_invoice_dates(period_flag = self.period_flag, manual_date_str=self.manual_date)
        self.assertEqual(period_start, datetime.strptime("2023-10-01", "%Y-%m-%d"))
        self.assertEqual(period_end, datetime.strptime("2023-12-31", "%Y-%m-%d"))

if __name__ == '__main__':
    unittest.main()
