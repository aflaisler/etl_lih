import boto3
import json

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
conn = boto3.Session(aws_access_key_id=sb_credentials['s3accessKeys']['AWS_ACCESS_KEY_ID'],
                     aws_secret_access_key=sb_credentials['s3accessKeys']['AWS_SECRET_ACCESS_KEY'],
                     region_name='eu-west-1')

s3 = conn.client('s3')

s3.list_objects_v2(Bucket='ai-sony-automation')

s3.upload_file('test.ipynb', 'ai-sony-automation', 'test.ipynb')
