#!/usr/bin/python
import sys
import os
import csv
import logging
import botocore
import boto3
import pymysql
import pandas
import numpy

# Connection for S3
s3 = boto3.client('s3')
s3_bucket = 'data-lake'

# Connection for RDS
rds_host = ""
name = ""
password = ""
db_name = ""
port = 3306
logger = logging.getLogger()
logger.setLevel(logging.INFO)


try:
    conn = pymysql.connect(host=rds_host, user=name,
                           passwd=password, db=db_name, connect_timeout=5)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# today_date = event['today_date']#pandas.to_datetime('today').strftime('%Y-%m-%d')#event['dayofdata']
# print(today_date)
list_tbl_files = ["site", "contains", "site_category"]


def scrape_handler(today_date, domain):
    print("Date of Data: " + today_date)
    global site_query, contains_query, site_category_query
    # "select fact_id, site_id, keyword_id, points from contains where date(current_dates)='"+today_date+"'"
    contains_query = "select fact_id, site_id, keyword_id, points from contains where site_id in (select distinct site_id from site where date(current_dates)='" + today_date + "' and page_url like '%" + domain + "%');"
    site_query = "select site_id, page_url, last_scrapped_date from site where date(current_dates)='" + today_date + "' and page_url like '%" + domain + "%'"
    site_category_query = "select s_c_id, site_id, domain, custom_segment_name from site_category where domain like '%" + domain + "%' and date(current_dates)='" + today_date + "'"

    with conn.cursor() as cur:
        for i in list_tbl_files:
            query = globals()[i + '_query']
            print(query)

            results = pandas.read_sql_query(sql=query, con=conn, coerce_float=False)
            # print(results)
            if i == "contains":
                results = results.fillna(0).astype({'fact_id': 'int', 'site_id': 'int', 'keyword_id': 'int', 'points': 'int'})
            if i == "visit":
                results = results.fillna(0).astype({'visit_id': 'int', 'user_id': 'int', 'site_id': 'int'})
            print("Total Data for " + str(i) + ": " + str(len(results)))
            file = results.to_csv("/tmp/" + str(i) + ".csv.gz", index=False, compression='gzip')
            s3_upload = s3.upload_file(Filename="/tmp/" + str(i) + ".csv.gz", Bucket=s3_bucket,
                                       Key="Segment_Data/RDS_To_RedShift_Data/" + str(i) + "/" + str(i) + ".csv.gz")

# scrape_handler(str(sys.argv[1]),str(sys.argv[2]))
