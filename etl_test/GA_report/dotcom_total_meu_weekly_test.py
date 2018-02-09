'''
Unit tests for dotcom-total-meu_weekly
To run the tests: make test
'''

import unittest as unittest
import dotcom_total_meu_weekly as report
import numpy as np

class TestReport(unittest.TestCase):

    def test_api_access(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        self.assertEqual(valid_access, 1)

    def test_date_range(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        json = report.get_report(analytics)
        # select unique nb of days
        # code here
        self.assertEqual(nb_days, 7)


if __name__ == '__main__':
    unittest.main()
