import boto3
from botocore.config import Config
import sys
import pandas as pd

chunksize = 10 ** 6

s3 = boto3.client('s3')#, aws_access_key_id = aws_access_id, aws_secret_access_key = aws_access_key, region_name= 'us-east-1')
s3_bucket = 'data-lake'

dconfig = Config(retries={'max_attempts': 20})
dynamodb = boto3.resource('dynamodb')#, aws_access_key_id = aws_access_id, aws_secret_access_key = aws_access_key, region_name= 'us-east-1', config=dconfig)
table = dynamodb.Table('ct_service')

paths = []


def remove_from_dynamo(dates):
    global number_of_files
    Key="Classified_Url/"+dates+"/"

    # Getting PATH of Data_Files
    req = s3.list_objects(Bucket=s3_bucket, Prefix=Key)#3 columns from glue processing
    try:
      for content in req['Contents']:
          if content['Key'][-1] == "z" and content['Key'][-2] == "g" and content['Key'][-3] == "." :
              # and content['Key'][-4] == "." :
              obj_dict = s3.get_object(Bucket=s3_bucket, Key=content['Key'])
              c = paths.append("s3://data-lake/"+content['Key'])
    except KeyError as k:
      print(k)
      pass

    for i in paths[:]:
      print(i)
      df2 = pd.read_csv(i, usecols=["page_url"], compression='gzip', header=0, sep=',', quotechar='"')
      urls = df2.values.tolist()
      for i in urls[:]:
        print(i)
        try:
          response = table.delete_item(Key={'page_url': i[0]})
          print(response)
          item = response['Item']['keywords']
          print(item)
        except Exception as e:
          print(e)

#remove_from_dynamo()
