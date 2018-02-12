import unittest
from socialbakers_api import api
from socialbakers_api import SocialNetworkObject as SNO
from unittest import TestCase
import json

class SocialnetworkObject(unittest.TestCase):
    global token, secret, fb_fields, ids
    credentials_path = "/Users/aymericflaisler/airflow/scripts/social_backers_api/etl_test/client/credentials.json"
    sb_credentials = json.load(open(credentials_path))
    token = sb_credentials['socialbakers']['token']
    secret = sb_credentials['socialbakers']['secret']
    api.SocialbakersApi.init(token, secret)
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
        SNO.FacebookObject.Metric.shares_count,
        SNO.FacebookObject.Metric.shares_count_by_paid_status,
        SNO.FacebookObject.Metric.shares_count_by_type,
        SNO.FacebookObject.Metric.user_posts_average_response_time,
        SNO.FacebookObject.Metric.user_posts_by_app,
        SNO.FacebookObject.Metric.user_posts_count,
        SNO.FacebookObject.Metric.user_posts_responded_by_time,
        SNO.FacebookObject.Metric.user_posts_responded_count,
        SNO.FacebookObject.Metric.user_posts_response_rate,
        SNO.FacebookObject.Metric.user_questions_average_response_time,
        SNO.FacebookObject.Metric.user_questions_count,
        SNO.FacebookObject.Metric.user_questions_responded_by_time,
        SNO.FacebookObject.Metric.user_questions_responded_count,
        SNO.FacebookObject.Metric.user_questions_response_rate,
    ]
    ids = ['127214327307013']

    def test_socialnetworkobject_time_range(self):
        fb = SNO.FacebookObject()
        self.assertEqual(1, fb.time_range('2017-01-01', '2017-01-02'))

    def test_socialnetworkobject_add_time(self):
        fb = SNO.FacebookObject()
        self.assertEqual('2017-01-03', fb.add_time('2017-01-02', 1))

    def test_socialnetworkobject_get_bulk_metrics_more90(self):
        fb = SNO.FacebookObject()
        self.assertTrue(fb.get_bulk_metrics('2016-01-01', '2016-06-30', ids, fb_fields)['success'])


if __name__ == '__main__':
    unittest.main()
