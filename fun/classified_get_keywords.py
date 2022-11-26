#!/usr/bin/python3
import sys
import os
import csv
import logging
import botocore
import boto3
import pymysql
import pandas
import numpy
cwd = os.getcwd()
#Connection for RDS
rds_host = " "
name = " "
password = ""
db_name = " "
port = 3306

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name,
                           passwd=password, db=db_name, connect_timeout=5)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

query = "select keyword from keywords"


def scrape_handler():
    # with conn.cursor() as cur:
    results = pandas.read_sql_query(sql=query, con=conn, coerce_float=False)
    results = results[~results.keyword.str.contains("\n")]
    results.to_csv(f"{cwd}/temp/keywords.txt", header=False, index=False, line_terminator=',')

scrape_handler()
