'''
Unit tests for dotcom-total-meu_weekly
To run the tests: make test
'''

import unittest as unittest
import dotcom_total_meu_weekly as report
import numpy as np
import pandas as pd
import os

class TestReport(unittest.TestCase):

    def test_api_access(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        self.assertEqual(valid_access, 1)

    def test_columns(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        data = report.get_report(analytics, '7daysAgo', 'yesterday', useResourceQuotas=False, page_token=0)
        df = report.format_response(data)
        columns = ['ga:country', 'ga:isoYearIsoWeek', 'ga:segment', 'ga:uniquePurchases', 'ga:users']
        self.assertEqual(sum(columns == df.columns.values), len(df.columns))

    def test_pagination(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        report.get_report(analytics, '30daysAgo', 'yesterday', useResourceQuotas=False, page_token=1000)

    def test_segment(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        data = report.get_report(analytics, '7daysAgo', 'yesterday', useResourceQuotas=False, page_token=0)
        df = report.format_response(data)
        self.assertEqual(len(df['ga:segment'].unique()), 3)

    def test_row_count(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        data = report.get_report(analytics, '7daysAgo', 'yesterday', useResourceQuotas=True, page_token=0)
        rc = data['reports'][0]['data']['rowCount']
        columns = ['ga:country', 'ga:isoYearIsoWeek', 'ga:segment', 'ga:uniquePurchases', 'ga:users']
        df = report.main(columns=columns, start_date='7daysAgo', end_date='yesterday')
        df.shape
        self.assertEqual(df.shape, (rc, len(columns)))

    def test_date_range(self):
        valid_access, analytics = report.initialize_analyticsreporting()
        response_page_token = 0
        # initialize dataframe for report
        df = pd.DataFrame(columns=['ga:country', 'ga:date', 'ga:segment', 'ga:uniquePurchases', 'ga:users'])
        while response_page_token is not None:
            response = report.get_report_test(analytics, start_date='7daysAgo', end_date='yesterday', page_token=response_page_token)
            df = df.append(report.format_response(response), ignore_index=True)
            if response['reports'][0].get('nextPageToken', None) is not None:
                response_page_token = response['reports'][0]['nextPageToken']
            else:
                response_page_token = None
        # Reset index (not actually necessary here)
        df.reset_index(drop=True, inplace=True)
        # select unique nb of days
        self.assertEqual(len(df['ga:date'].unique()), 7)


if __name__ == '__main__':
    unittest.main(verbosity=2)
    # to run test in ipython:
    suite = unittest.TestLoader().loadTestsFromTestCase(TestReport)
    unittest.TextTestRunner(verbosity=2).run(suite)
