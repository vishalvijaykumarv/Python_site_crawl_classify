import json
import sys
import s3fs
import logging
import pymysql
import time
from multiprocessing import Pool
import boto3
import pandas as pd
import numpy as np
from pandas.io import sql
import json
import datetime
from datetime import datetime
import re
import gzip
import sys
import json
import boto3
import os
import configparser
from urllib.parse import urlparse
import math
from io import StringIO
from botocore.config import Config
from botocore.exceptions import ClientError
cwd = os.getcwd()
config = configparser.RawConfigParser()
config.read(f"{cwd}/scripts/config.cfg")
RDS_config = dict(config.items('RDS'))



chunksize = 10 ** 6



aws_access_id = os.environ.get('AWS_ACCESS_ID')
aws_access_key = os.environ.get('AWS_ACCESS_KEY')
# dates = str(datetime.today().strftime('%Y-%m-%d'))#os.environ.get('today_date')#str(datetime.today().strftime('%Y-%m-%d'))
s3 = boto3.client(
    's3')  # , aws_access_key_id = aws_access_id, aws_secret_access_key = aws_access_key, region_name= 'us-east-1')
dconfig = Config(retries={'max_attempts': 20})
dynamodb = boto3.resource(
    'dynamodb')  # , aws_access_key_id = aws_access_id, aws_secret_access_key = aws_access_key, region_name= 'us-east-1', config=dconfig)
table = dynamodb.Table('ct_service')
api = "https://ct2.Classify_Namemedia.net/?url="
# rds_host = RDS_config['host_add']
# rds_host_writer = " "
# rds_host_reader = ""
# rds_username = " "
# rds_password = "  "
# rds_database = " "
# rds_port = 3306

rds_username=RDS_config['username']
rds_password=RDS_config['password']
rds_database=RDS_config['database']
rds_port=RDS_config['host_prt']
rds_host=RDS_config['host_add']

lis = []


def lambda_handler(dates, file_url):
    global site_id
    global k_id
    global cs_name

    # try:
    #     con_reader = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    #     cur_reader = con_reader.cursor()
    # except Exception as e:
    #     print(str(e))
    #     print("ERROR: Unexpected error: Could not connect to MySql instance.")
    #     sys.exit()

    # RDS Reader
    try:
        con_reader = pymysql.connect(host=rds_host, user=rds_username, passwd=rds_password, db=rds_database, connect_timeout=5)
        cur_reader = con_reader.cursor()
        con_reader.ping(reconnect=True)
    except Exception as e:
        print(str(e))
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()

    print(dates)
    # print("SUCCESS: Connection to RDS mysql instance succeeded")
    print(file_url)
    df2 = pd.read_csv(file_url, usecols=["page_url"], chunksize=chunksize, compression='gzip', header=0, sep=',',
                      quotechar='"')
    for df in df2:
        df.dropna(how='any', inplace=True)
        df1 = df[['page_url']]

        site_q = "insert ignore into site (page_url,last_scrapped_date,current_dates) values (%s,\"" + str(
            dates) + "\",\"" + str(dates) + "\")"
        try:
            print(site_q)
            print(cur_reader.executemany(site_q, df1.values.tolist()))
            con_reader.commit()
        except Exception as e:
            print(e)
        con_reader.rollback()

        for i in df.values.tolist():
            # print(i)
            page_url_escaped = str(i[0])
            domain_name = urlparse(page_url_escaped).netloc
            site_id_q = "select site_id from site where page_url=\"" + page_url_escaped + "\""
            print(site_id_q)
            try:
                cur_reader.execute(site_id_q)
                (site_id) = str(cur_reader.fetchone())
                site_id = site_id.replace(',', '')
                print(site_id)
                # print("-->"+i[1])
            except Exception as e:
                print(e)
                con_reader.rollback()

            if not urlparse(page_url_escaped).scheme:
                page_url_escaped = 'http://' + page_url_escaped
                print(f"page_url_escaped is = {page_url_escaped}")
            try:
                response = table.get_item(Key={'page_url': page_url_escaped})
                print(f"response value = {response}")
                item = response['Item']['keywords']
                data = item
                empty = []
                print(f"data length is = {len(data)}")
                if (len(data) > 0):
                    for key, value in data.items():
                        keywordsid2 = "select id from keywords where keyword=\"" + key + "\""
                        # print(f"keywordsid2 is = {keywordsid2}")
                        try:
                            cur_reader.execute(keywordsid2)
                            (ktesting2) = str(cur_reader.fetchone())
                            k_id = ktesting2.replace(',', '')
                        except Exception as e:
                            print(e)
                            con_reader.rollback()
                        keyword_contains_q2 = "insert ignore into contains (site_id, keyword_id, points,current_dates) values (" + str(
                            site_id) + "," + str(k_id) + ", \"" + str(value) + "\",\"" + str(dates) + "\")"
                        try:
                            print(keyword_contains_q2)
                            cur_reader.execute(keyword_contains_q2)
                            con_reader.commit()
                        except Exception as e:
                            print(e)
                            con_reader.rollback()

                        keywordsid = "select distinct(name) from custom_segments left join segment on custom_segments.id=segment.custom_segment_id " \
                                     "left join keywords on keywords.id=segment.keyword_id where keywords.keyword=\"" + key + "\""
                        try:
                            cur_reader.execute(keywordsid)
                            k_testing = str(cur_reader.fetchone())
                            cs_name = k_testing.replace(',', '')
                            cs_name = str(cs_name).replace('PrimeAudience – DTC Interest In ', '').replace(
                                'PrimeAudience – HCP Interest In ', '').replace('PrimeAudience – ', '').replace(
                                'PrimeAudience – DTC ', '').replace('PrimeAudience – MF ', '').replace(
                                'PrimeAudience – PV ', '')
                        except Exception as e:
                            print(e)
                            con_reader.rollback()
                        site_category_q = "insert ignore into site_category (site_id, domain, custom_segment_name, current_dates) values (" + str(
                            site_id) + ",'" + str(domain_name) + "'," + str(cs_name) + ",\"" + str(
                            dates) + "\") ON DUPLICATE KEY UPDATE current_dates=\"" + str(dates) + "\""
                        try:
                            print(site_category_q)
                            # print(site_category_q)
                            print(f"site category status = {cur_reader.execute(site_category_q)}")
                            con_reader.commit()
                        except Exception as e:
                            print(e)
                            con_reader.rollback()
                else:
                    empty.append(page_url_escaped)

            except KeyError:
                lis.append(page_url_escaped)

            except ClientError as error:
                time.sleep(10)
                print(f"Dynamo db side error = {error}")
                # Put your error handling logic here
                # raise error
            finally:
                pass

            page_url_escaped = page_url_escaped.replace("'", "''")

# lambda_handler(str(sys.argv[1]), str(sys.argv[2]))
