#!/usr/bin/env python3

import unittest
import sys
import os
import datetime
from datetime import date

import pandas as pd
# from pandas._testing import assert_frame_equal
# from pandas.testing import assert_extension_array_equal

lib_path = os.path.abspath('.')
sys.path.append(lib_path)

import numpy as np

from tasks import walmart_quarterly as wq

class TestWalmartQuarterly(unittest.TestCase):

    def setUp(self):
        cols = ["bin+pcn", "unique identifier",	"npi", "claim authorization number", "rx#", "service date", \
        	    "refill number", "ndc", "drug name", "generic indicator (multi-source indicator)", \
                "total quantity dispensed",	"total days supply", "dispensing pharmacy u&c price", \
                "taxes paid", "administration fee",	"patient amount", "average wholesale price (awp)", \
                "contracted_nadactr", "ingredient cost paid", "dispensing fee paid", "excluded claim indicator", \
                "exclusion reason", "reversal indicator", "fill_type"]
        results = pd.DataFrame(columns = cols)

        brand_generic = ['brand', 'generic']
        days_supply = [15,60,90,100,140]
        excluded = ['Y','N']
        nadac_awp = ['nadac','awp']

        for bg in brand_generic:
            for ds in days_supply:
                for x in excluded:
                    for nawp in nadac_awp:
                        results.loc[len(results)] = ["019876+0000000000","019876",1891712006, "189171200662015140", \
                                    6201514, datetime.datetime(2021, 9, 21) , 0, "68645058259", \
                                    "metFORMIN HCl", bg, 180, ds, 10, 0, 99, 10, 126.78,	289.62, \
                                    10,	0, x, '',	'B1', nawp]
        self.transactions = results

    def test_breakdown_quantity(self):
         #assert_frame_equal(self.transactions, wq.breakdown_quantity(self.transactions, 'brand', 'ingredient cost paid'))
        self.assertEqual([80, 80, 40], wq.breakdown_quantity(self.transactions, 'brand', 'ingredient cost paid'))
        self.assertEqual([1014.24, 1014.24, 507.12], wq.breakdown_quantity(self.transactions, 'brand', 'average wholesale price (awp)'))
        self.assertEqual([80, 80, 40], wq.breakdown_quantity(self.transactions, 'generic', 'ingredient cost paid'))
        self.assertEqual([1014.24, 1014.24, 507.12], wq.breakdown_quantity(self.transactions, 'generic', 'average wholesale price (awp)'))

    def test_previous_quarter(self):
        self.assertEqual(wq.previous_quarter(datetime.date(2121,10,22)),'DATE RANGE: 2121-07-01 - 2121-09-30')
        self.assertEqual(wq.previous_quarter(datetime.date(2121,1,1)),'DATE RANGE: 2120-10-01 - 2120-12-31')
        self.assertEqual(wq.previous_quarter(datetime.date(2121,4,1)),'DATE RANGE: 2121-01-01 - 2121-03-31')
        self.assertEqual(wq.previous_quarter(datetime.date(2121,7,1)),'DATE RANGE: 2121-04-01 - 2121-06-30')
        self.assertEqual(wq.previous_quarter(datetime.date(2121,10,1)),'DATE RANGE: 2121-07-01 - 2121-09-30')

    def test_breakdown_results(self):
        np.testing.assert_array_equal(np.array([80, 80, 40, 80, 80, 40]), wq.breakdown_results(self.transactions, 'ingredient cost paid'))
        np.testing.assert_array_equal(np.array([1014.24, 1014.24, 507.12, 1014.24, 1014.24, 507.12]), wq.breakdown_results(self.transactions, 'average wholesale price (awp)'))

    def test_count_quantity(self):
        self.assertEqual([8, 8, 4], wq.count_quantity(self.transactions, 'brand'))
        self.assertEqual([8, 8, 4], wq.count_quantity(self.transactions, 'generic'))

    def test_count_results(self):
        np.testing.assert_array_equal(np.array([2, 2, 1, 2, 2, 1]), wq.count_results(self.transactions, 'nadac'))
        np.testing.assert_array_equal(np.array([2, 2, 1, 2, 2, 1]), wq.count_results(self.transactions, 'awp'))

    def test_fill_type_breakdown(self):
         #assert_frame_equal(self.transactions, wq.breakdown_quantity(self.transactions, 'brand', 'ingredient cost paid'))
        np.testing.assert_array_equal(np.array([20, 20, 10, 20, 20, 10]), wq.fill_type_breakdown(self.transactions, 'ingredient cost paid', 'nadac'))
        np.testing.assert_array_equal(np.array([253.56, 253.56, 126.78, 253.56, 253.56, 126.78]), wq.fill_type_breakdown(self.transactions, 'average wholesale price (awp)', 'nadac'))
        np.testing.assert_array_equal(np.array([20, 20, 10, 20, 20, 10]), wq.fill_type_breakdown(self.transactions, 'ingredient cost paid', 'awp'))
        np.testing.assert_array_equal(np.array([253.56, 253.56, 126.78, 253.56, 253.56, 126.78]), wq.fill_type_breakdown(self.transactions, 'average wholesale price (awp)', 'awp'))

    def test_get_variance(self):
        contracted = np.array([253.56, 253.56, 126.78, 253.56, 253.56, 126.78])
        actual = np.array([53.56, 23.56, 26.78, 2.56, 253.50, 1.78])
        np.testing.assert_allclose(np.array([200,230,100,251,0.06,125]), wq.get_variance(contracted, actual))

    def test_get_actual_awp_rate(self):
        np.testing.assert_array_equal(np.array([0.9211232055529264, 0.9211232055529264, 0.9211232055529264, 0.9211232055529264, 0.9211232055529264, 0.9211232055529264]),wq.get_actual_awp_rate(self.transactions))

    def test_get_awp_variant_amount(self):
        actual_awp_rate = [0.125, 0.137, 0.13, 0.19,  0.2,   0.2  ]
        actual_awp = [112,343,2343,2343,234,54325]
        np.testing.assert_allclose(np.array([-1.12 , 0.686, -11.715, -23.43 , 0 , 0  ]), wq.get_awp_variant_amount(actual_awp,actual_awp_rate))

    def test_get_variant_amount(self):
        amount = [112,343,2343,2343,234,54325]
        np.testing.assert_array_equal(np.array([112,343,2343,2343,234,54325,59700]),wq.get_variant_amount(amount))

    def test_get_afer(self):
        self.assertEqual(99,wq.get_afer(self.transactions))

    def test_get_dispensing_fees(self):
        np.testing.assert_array_equal([1.01,  1.01,  1.01,  1.01, 1.01,  1.01 ],wq.get_dispensing_fees('awp'))
        np.testing.assert_array_equal([1.01,  1.01,  1.01,  8.50, 15.00, 40.00],wq.get_dispensing_fees('nadac'))

if __name__ == '__main__':
    unittest.main()
