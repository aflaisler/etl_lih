from __future__ import division
import numpy as np
import pandas as pd
import json
import os
from socialbakers_api import api
from socialbakers_api import SocialNetworkObject as SNO
from datetime import datetime, timedelta
import collections
from pymongo import MongoClient


weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
            'Friday', 'Saturday', 'Sunday']


# Helper to get last week dates (Monday to Sunday)
def get_previous_week_date(dayname, start_=False):
    start_date = datetime.today().date()
    day_num = start_date.weekday()
    day_num_target = weekdays.index(dayname)
    days_ago = (7 + day_num - day_num_target) % 7
    if days_ago == 0:
        days_ago = 7
    # start date must be > 7 days
    elif (days_ago < 7) & (start_ == True):
        days_ago += 7
    target_date = start_date - timedelta(days=days_ago)
    return target_date


# Initialize SocialbakersApi and return a FacebookObject
def init_socialbackers_api(credentials_path="client/credentials.json"):
    # get the socialbakers api secret key use the absolute path in production
    try:
        filepath = os.path.dirname(os.path.realpath(__file__)) + "/" + credentials_path
    except:
        filepath = credentials_path
    sb_credentials = json.load(open(filepath))
    token = sb_credentials['socialbakers']['token']
    secret = sb_credentials['socialbakers']['secret']
    # Initialize API
    api.SocialbakersApi.init(token, secret)
    # There is an object for each Social Network(Facebook, Twitter, Instagram, Youtube, Pinterest)
    fb = SNO.FacebookObject()
    return fb


# Get profile IDs by labels
def get_profile_id_by_labels(profiles_json, label=None):
    """
    Takes the get_profiles json from socialbakers and return a list of profile_id by labels
    """
    ls_profile_id_labels = [profiles_json['profiles'][i]['id'] for i in range(len(profiles_json['profiles']))
                            if label in profiles_json['profiles'][i]['sbks_labels']]


# Sum results over the period
def df_results_period(result):
    """
    - Take a json of metrics return a pandas df grouping by profile_id
    - Calculate couple of metrics for the report
    """
    ls_dict = []
    for i in range(len(result['profiles'])):
        # print result['profiles'][i]['data']
        counter = collections.Counter()
        for d in result['profiles'][i]['data']:
            d['profile_id'] = result['profiles'][i]['id']
            # print d
            ls_dict.append(d)
    # Converting the list of dictionary into a pandas DataFrame
    df = pd.DataFrame(ls_dict)
    # droping the date
    df.drop(['date'], axis=1, inplace=True)
    # grouping by profile_id
    df_group = df.loc[:, ['profile_id',
                          'fans_change',
                          'interactions_per_1000_fans',
                          'interactions_count',
                          'reactions_count',
                          'shares_count',
                          'comments_count',
                          'page_posts_count',
                          'user_posts_count',
                          'user_posts_responded_count'
                          ]].groupby(['profile_id'], as_index=False).sum()

    # Avg total fans
    df_total = df.loc[:, ['profile_id', 'fans_count_lifetime']].groupby(['profile_id'], as_index=False).mean()
    # Do a SQL like join
    df_group = df_group.merge(df_total, on=['profile_id'])
    # renaming columns
    df_group.rename(columns={"fans_change": "Total Change in Total Fans",
                             "fans_count_lifetime": "Total Fans",
                             "interactions_count": "Sum of Interactions",
                             "reactions_count": "Sum of Reactions",
                             "shares_count": "Sum of Shares",
                             "comments_count": "Sum of Comments",
                             "page_posts_count": "Sum of Page Posts",
                             "user_posts_count": "Sum of User Posts",
                             "user_posts_responded_count": "Sum of Responded User Posts"}, inplace=True)
    # Relative Change in Total Fans
    df_group['Relative Change in Total Fans'] = np.round(df_group["Total Change in Total Fans"] * 100 / df_group["Total Fans"], 2)
    # df_group
    # Avg Interactions per 1000 Fans per Post
    df_group['Avg Interactions per 1000 Fans per Post'] = np.nan
    return df_group


# Helper to save report to mongodb
def mongo_storage_helper(collection_name, json):
    client = MongoClient()
    client.socialbackers[collection_name].insert_many(json)


if __name__ == "__main__":
    # List all profile labels
    ls_labels = ["APAC", "Competitor", "Competitor Central", "Competitor Local", "East Asia",
                 "Europe", "Global", "LAM", "MEA", "NAM", "Sony Mobile", "Sony Mobile Local"]

    # fb fields to retrieve from the api
    fb_fields = [SNO.FacebookObject.Metric.fans_count_lifetime,
                 SNO.FacebookObject.Metric.fans_change,
                 SNO.FacebookObject.Metric.interactions_per_1000_fans,
                 SNO.FacebookObject.Metric.interactions_count,
                 SNO.FacebookObject.Metric.reactions_count,
                 SNO.FacebookObject.Metric.shares_count,
                 SNO.FacebookObject.Metric.comments_count,
                 SNO.FacebookObject.Metric.page_posts_count,
                 SNO.FacebookObject.Metric.user_posts_count,
                 SNO.FacebookObject.Metric.user_posts_responded_count]

    # Initialize the api and get the list of profiles in the account
    print "API initialisation..."
    fb = init_socialbackers_api(credentials_path="client/credentials.json")
    profiles = fb.get_profiles()
    profiles_json = json.loads(profiles)
    ids_ = [profiles_json['profiles'][i]['id'] for i in range(len(profiles_json['profiles']))]

    # Create dictionary with label as key and ids as values
    dict_label_ids = {label: get_profile_id_by_labels(profiles_json, label) for label in ls_labels}

    # get the previous week date
    start = get_previous_week_date('Monday', start_=True).strftime('%Y-%m-%d')
    end = get_previous_week_date('Sunday').strftime('%Y-%m-%d')

    # Get Metrics from each profile waiting of 2sec to bypass the 25 profiles limit
    print "Getting the results..."
    # result = fb.get_metrics_split_profiles(start, end, ids_, fb_fields)
    # testing only on Sony Mobile
    sony_AR = "134604246572113"
    id_sony_global = "35313373389"
    result = fb.get_metrics_split_profiles(start, end, [id_sony_global, sony_AR], fb_fields)
    # result['profiles']

    # Dump the results into a json
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    # with open(dir_path + '/data_S.json', 'w') as outfile:
    # json.dump(result, outfile)

    # Store the results into mongodb
    mongo_storage_helper(collection_name="fb_platform_overview", json=result['profiles'])

    # Load the results
    # result = json.load(open('data.json'))

    # Sum results over the period and return a pandas dataframe
    df = df_results_period(result)
    print "Extract of the report:"
    print df.head()


#
