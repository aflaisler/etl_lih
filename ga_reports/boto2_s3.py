from boto.s3.connection import S3Connection
import json
import os

def s3_access_keys(credentials_path="client/credentials.json"):
    # get the socialbakers api secret key use the absolute path in production
    try:
        filepath = os.path.dirname(os.path.realpath(__file__)) + "/" + credentials_path
    except:
        filepath = credentials_path
    sb_credentials = json.load(open(filepath))
    key = sb_credentials['s3accessKeys']['AWS_ACCESS_KEY_ID']
    secret = sb_credentials['s3accessKeys']['AWS_SECRET_ACCESS_KEY']
    return key, secret


key, secret = s3_access_keys(credentials_path="client/credentials.json")
conn = S3Connection(key, secret)

bucket = conn.get_bucket('ai-sony-automation')
for obj in bucket.get_all_keys():
    print(obj.key)
