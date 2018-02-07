import numpy as np
import pandas as pd
import json
import os
from socialbakers import api
from socialbakersobjects import SocialNetworkObject as SNO
from datetime import datetime, timedelta

weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
            'Friday', 'Saturday', 'Sunday']

# Helper to get last week dates (Monday to Sunday)
def get_previous_byday(dayname, start_=False):
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


# get the previous week date
start = get_previous_byday('Monday', start_=True).strftime('%Y-%m-%d')
end = get_previous_byday('Sunday').strftime('%Y-%m-%d')

start
end
# get the socialbakers api secret key
credentials_path = "./client/credentials.json"
sb_credentials = json.load(open(credentials_path))
token = sb_credentials['socialbakers']['token']
secret = sb_credentials['socialbakers']['secret']

# Initialize API
api.SocialbakersApi.init(token, secret)

# There is an object for each Social Network(Facebook, Twitter, Instagram, Youtube, Pinterest)
fb = SNO.FacebookObject()

# Check Fields Available in . / socialbakers / objects / socialnetworkobject

fb_fields = [
    SNO.FacebookObject.Metric.comments_count_by_paid_status,
    SNO.FacebookObject.Metric.comments_count_by_type,
    SNO.FacebookObject.Metric.fans_change,
    SNO.FacebookObject.Metric.fans_count_lifetime,
    SNO.FacebookObject.Metric.fans_count_lifetime_by_country,
    SNO.FacebookObject.Metric.interactions_count,
    SNO.FacebookObject.Metric.interactions_count_by_paid_status,
    SNO.FacebookObject.Metric.interactions_count_by_type,
    SNO.FacebookObject.Metric.interactions_per_1000_fans,
    SNO.FacebookObject.Metric.interactions_per_1000_fans_by_type,
    SNO.FacebookObject.Metric.page_posts_by_app,
    SNO.FacebookObject.Metric.page_posts_count,
    SNO.FacebookObject.Metric.page_posts_count_by_paid_status,
    SNO.FacebookObject.Metric.page_posts_count_by_type,
    SNO.FacebookObject.Metric.reactions_count,
    SNO.FacebookObject.Metric.reactions_count_by_paid_status,
    SNO.FacebookObject.Metric.reactions_count_by_reaction_type,
    SNO.FacebookObject.Metric.reactions_count_by_type,
    SNO.FacebookObject.Metric.shares_count
    # SNO.FacebookObject.Metric.shares_count_by_paid_status,
    # SNO.FacebookObject.Metric.shares_count_by_type,
    # SNO.FacebookObject.Metric.user_posts_average_response_time,
    # SNO.FacebookObject.Metric.user_posts_by_app,
    # SNO.FacebookObject.Metric.user_posts_count,
    # SNO.FacebookObject.Metric.user_posts_responded_by_time,
    # SNO.FacebookObject.Metric.user_posts_responded_count,
    # SNO.FacebookObject.Metric.user_posts_response_rate,
    # SNO.FacebookObject.Metric.user_questions_average_response_time,
    # SNO.FacebookObject.Metric.user_questions_count,
    # SNO.FacebookObject.Metric.user_questions_responded_by_time,
    # SNO.FacebookObject.Metric.user_questions_responded_count,
    # SNO.FacebookObject.Metric.user_questions_response_rate
]

# Get profiles added to your account
profiles = fb.get_profiles()


profiles_json = json.loads(profiles)

profiles


id_ = profiles_json['profiles'][0]['id']

ids_ = [profiles_json['profiles'][i]['id']
        for i in range(len(profiles_json['profiles']))]

# Get Metrics from each profile
result = fb.get_metrics(start, end,
                        ids_[:5], fb_fields)

start

end
result
