'''
Unit tests for dotcom-total-meu_weekly
To run the tests: make test
'''

import unittest as unittest
import dotcom_total_meu_weekly as report
import numpy as np
import pandas as pd

class TestReport(unittest.TestCase):

    def test_api_access(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        self.assertEqual(valid_access, 1)

    def test_columns(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        data = report.get_report(analytics, '7daysAgo', 'today', useResourceQuotas=False, page_token=0)
        df = report.format_response(data)
        columns = ['ga:country', 'ga:isoYearIsoWeek', 'ga:segment', 'ga:uniquePurchases', 'ga:users']
        self.assertEqual(sum(columns == df.columns.values), len(df.columns))

    def test_pagination(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        report.get_report(analytics, '30daysAgo', 'today', useResourceQuotas=False, page_token=1000)
    #
    # def test_date_range(self):
    #     valid_access, analytics = report.initialize_analyticsreporting()
    #     json = report.get_report(analytics, '7daysAgo', 'today')
    #     # select unique nb of days
    #     # code here
    #     self.assertEqual(nb_days, 7)


if __name__ == '__main__':
    unittest.main()
