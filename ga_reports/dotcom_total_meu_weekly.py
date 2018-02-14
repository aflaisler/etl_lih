from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import pandas as pd
from datetime import datetime
import os

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
# Sony all traffic unfiltered
VIEW_ID = '56018391'


def initialize_analyticsreporting(credentials_path="client/GA_API_service_account.json"):
    """Initializes an Analytics Reporting API V4 service object.
    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    # get ga credentials and use the absolute path in production
    try:
        filepath = os.path.dirname(os.path.realpath(__file__)) + "/" + credentials_path
    except:
        filepath = credentials_path
    credentials = ServiceAccountCredentials.from_json_keyfile_name(filepath, SCOPES)
    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return 1, analytics


def get_report(analytics, start_date, end_date, useResourceQuotas=True, page_token=0):
    """Queries the Analytics Reporting API V4.
    Args:
      analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
      The Analytics Reporting API V4 response.
    """
    useResourceQuotas_bool = 'true' if useResourceQuotas else 'false'
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageToken': str(page_token),
                    'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                    'metrics': [{'expression': 'ga:users'},
                                {'expression': 'ga:uniquePurchases'}],
                    'dimensions': [{'name': 'ga:isoYearIsoWeek'},
                                   {'name': 'ga:country'},
                                   {'name': 'ga:segment'}],
                    'segments': [{'segmentId': 'gaid::bYSo65dpSxyyR1CjanHDyQ'},
                                 {'segmentId': 'gaid::-1'},
                                 {'segmentId': 'gaid::Dhbf_lhSRm-XK2oFGM5POg'}]
                }],
            'useResourceQuotas': useResourceQuotas_bool,
        }).execute()


def get_report_test(analytics, start_date, end_date, useResourceQuotas=False, page_token=0):
    """Test function using "get_report" (above) but adding the date at a day level
        Queries the Analytics Reporting API V4.
    Args:
      analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
      The Analytics Reporting API V4 response.
    """
    useResourceQuotas_bool = 'true' if useResourceQuotas else 'false'
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageToken': str(page_token),
                    'samplingLevel': 'LARGE',
                    'pageSize': '10000',
                    'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                    'metrics': [{'expression': 'ga:users'},
                                {'expression': 'ga:uniquePurchases'}],
                    'dimensions': [{'name': 'ga:date'},
                                   {'name': 'ga:country'},
                                   {'name': 'ga:segment'}],
                    'segments': [{'segmentId': 'gaid::bYSo65dpSxyyR1CjanHDyQ'},
                                 {'segmentId': 'gaid::-1'},
                                 {'segmentId': 'gaid::Dhbf_lhSRm-XK2oFGM5POg'}]
                }],
            'useResourceQuotas': useResourceQuotas_bool,
        }).execute()

def format_response(response):
    """Take a reponse from Google analytics report api v4 and format it into a
    pandas dataframe
    Args:
        GA reporting api v4 response (json)
    Returns:
        a pandas dataframe
    """
    ls = []
    # get report data
    for report in response['reports']:
        # set column headers
        columnHeader = report['columnHeader']
        dimensionHeaders = columnHeader['dimensions']
        metricHeaders = columnHeader['metricHeader']['metricHeaderEntries']
        rows = report['data']['rows']

    for row in rows:
        # create dict for each row
        dict_ = {}
        dimensions = row['dimensions']
        dateRangeValues = row['metrics']
        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
            dict_[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
            for metric, value in zip(metricHeaders, values['values']):
                # set int as int, float a float
                if ',' in value or ',' in value:
                    dict_[metric['name']] = float(value)
                else:
                    dict_[metric['name']] = int(value)
        ls.append(dict_)
    df = pd.DataFrame(ls)
    return df


def main(columns, columns_out, start_date, end_date):
    """ Init. GA reporting api and query it until all the results have been returned.
        By default the page size is set to 1000 in the get_report function.

    Args:
        columns of the output dataframe, start and end date of the query
    Returns:
        pandas df
    """
    valid_access, analytics = initialize_analyticsreporting()
    # start getting report at index 0
    response_page_token = 0
    # initialize dataframe for report
    df = pd.DataFrame(columns=columns)
    while response_page_token is not None:
        response = get_report(analytics, start_date=start_date, end_date=end_date, page_token=response_page_token)
        df = df.append(format_response(response))
        if response['reports'][0].get('nextPageToken', None) is not None:
            response_page_token = response['reports'][0]['nextPageToken']
        else:
            response_page_token = None
    # reorder df
    df = df[columns]
    # rename columns to fit discovery etls
    df.columns = columns_out
    # Reset index (not actually necessary here)
    df.reset_index(drop=True, inplace=True)
    # check quotas
    print response.get('resourceQuotasRemaining', '')
    # return dataframe with all the api replies concatanated
    return df


if __name__ == '__main__':
    # columns from google analytics api fields name
    ga_columns = ['ga:isoYearIsoWeek', 'ga:country', 'ga:segment', 'ga:users', 'ga:uniquePurchases']
    # output column names
    out_columns = ['ISO Week of ISO Year', 'Country', 'Segment', 'Users', 'Unique Purchases']
    # call all the methods
    df = main(columns=ga_columns, columns_out=out_columns, start_date='7daysAgo', end_date='yesterday')
    filepath = os.path.dirname(os.path.realpath(__file__)) + "/"
    df.to_csv(filepath + 'data/dotcom_total_meu_weekly_{}.csv'.format(datetime.today().strftime('%m%d_%H%M')),
              encoding='utf-8', index=False)
    df.to_csv(filepath + 'data/dotcom_total_meu_weekly.csv', encoding='utf-8', index=False)
