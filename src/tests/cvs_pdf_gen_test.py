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

from tasks import cvs_quarterly as cq

class TestCVSPDFGen(unittest.TestCase):

    def setUp(self):
        cols = ["period_start",	"period_end", "start_date", "end_date", "days_supply", "drug type",	\
                "basis_of_reimbursement_source", "claim count", "total ingredient cost", "total administration fee", \
                "total awp", "actual effective rate", "target effective rate", "effective rate variance","dollar variance"]

        results = pd.DataFrame(columns = cols)

        # taken from Q4 2022
        results.loc[0] = ["10/01/2022", "12/31/2022", "2022-10-01 00:00:00", "2022-12-31 00:00:00","Any", "Any OTC brand", \
                          "AWP", 429, 13455.6, 1190.69, \
                          15386.08, 12.5469, 12.55, -0.003074, -0.4730]

        self.summary_results = results

    def test_add_exempt_generics(self):
        missing_row_results = self.summary_results
        period_start = '20221001'
        period_end = '20221231'
        exempt_generic_row = pd.DataFrame({'period_start': period_start, 'period_end': period_end, \
                                           'start_date': period_start, 'end_date': period_end, \
                                           'days_supply': 'Any', 'drug type': 'exempt generic', \
                                           'basis_of_reimbursement_source': '','claim count': 0, \
                                           'total ingredient cost': 0, 'total administration fee': 0, \
                                           'total awp': 0, 'actual effective rate':  '', \
                                           'target effective rate': '', 'effective rate variance': 0, \
                                           'dollar variance': 0}, index=[len(missing_row_results)])
        missing_row_results = pd.concat([missing_row_results, exempt_generic_row],ignore_index=True)


        np.testing.assert_array_equal(missing_row_results, cq.add_exempt_generics(period_start,period_end,self.summary_results))
        #Calling a second time to validate nothing happens when there is an exempt generic row
        np.testing.assert_array_equal(missing_row_results, cq.add_exempt_generics(period_start,period_end,self.summary_results))

if __name__ == '__main__':
    unittest.main()
