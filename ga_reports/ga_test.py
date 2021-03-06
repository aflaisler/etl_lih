"""Hello Analytics Reporting API V4."""


from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
# remember to add access right at view level
VIEW_ID = '56018391'


def initialize_analyticsreporting(credentials_path="client/GA_API_service_account.json"):
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    try:
        filepath = os.path.dirname(os.path.realpath(__file__)) + "/" + credentials_path
    except:
        filepath = credentials_path
    credentials = ServiceAccountCredentials.from_json_keyfile_name(filepath, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def get_report(analytics):
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
                    'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
                    'metrics': [{'expression': 'ga:sessions'}],
                    'dimensions': [{'name': 'ga:country'}]
                }]
        }
    ).execute()


def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
      response: An Analytics Reporting API V4 response.
    """
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get(
            'metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
                print header + ': ' + dimension

            for i, values in enumerate(dateRangeValues):
                print 'Date range: ' + str(i)
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    print metricHeader.get('name') + ': ' + value


def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    print_response(response)


if __name__ == '__main__':
    main()
