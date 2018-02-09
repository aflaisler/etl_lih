from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

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


def get_report(analytics, start_date, end_date):
    """Queries the Analytics Reporting API V4.

    Args:
      analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
      The Analytics Reporting API V4 response.
    """
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                    'metrics': [{'expression': 'ga:users'},
                                {'expression': 'ga:uniquePurchases'}],
                    'dimensions': [{'name': 'ga:isoYearIsoWeek'},
                                   {'name': 'ga:country'},
                                   {'name': 'ga:segment'}],
                    'segments': [{'segmentId': 'gaid::bYSo65dpSxyyR1CjanHDyQ'},
                                 {'segmentId': 'gaid::-1'},
                                 {'segmentId': 'gaid::Dhbf_lhSRm-XK2oFGM5POg'}]
                }]
        }).execute()


def format_response(response):
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


def main():
    valid_access, analytics = initialize_analyticsreporting()
    response = get_report(analytics, '7daysAgo', 'today')
    format_response(response)


if __name__ == '__main__':
    main()


start_date, end_date = '7daysAgo', 'today'

r = analytics.reports().batchGet(
    body={
        'reportRequests': [
            {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                'metrics': [{'expression': 'ga:users'},
                            {'expression': 'ga:uniquePurchases'}],
                'dimensions': [{'name': 'ga:isoYearIsoWeek'},
                               {'name': 'ga:country'},
                               {'name': 'ga:segment'}],
                'segments': [{'segmentId': 'gaid::bYSo65dpSxyyR1CjanHDyQ'},
                             {'segmentId': 'gaid::-1'},
                             {'segmentId': 'gaid::Dhbf_lhSRm-XK2oFGM5POg'}]
            }]
    }).execute()


analytics.reports().batchGet(1000)
