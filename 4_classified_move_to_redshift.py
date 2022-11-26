import psycopg2
from fun import classified_rds_to_s32 as rds
import pandas as pd
import sys

#### RedShift ####
DATABASE = ""
USER = ""
PASSWORD = ""
HOST = ""
PORT = "5439"
SCHEMA = "public"

############ CONNECTING AND CREATING SESSIONS ############
conn = psycopg2.connect("host= "+HOST+" port= "+PORT+" user= "+USER+" password= "+PASSWORD+" dbname= "+DATABASE+"")
cur = conn.cursor()

SetPath = "SET search_path TO "+ SCHEMA
cur.execute(SetPath)
###########################################################

#### S3 ####
S3_FULL_PATH = 's3://data-lake/RDS_To_RedShift_Data/'
ARN_CREDENTIALS = ''
REGION = 'us-east-1'

list_tbl_files = ["contains", "site", "site_category"]

def main_handler(table):
    ############ RUNNING COPY ############
    S3_PATH = S3_FULL_PATH+table+"/"+table+".csv.gz"
    print(S3_PATH)
    copy_command = '''
    COPY "%s"
    FROM '%s'
    credentials 'aws_iam_role=%s'
    delimiter ',' COMPUPDATE ON gzip csv 
    region '%s'
    quote '"'
    IGNOREHEADER 1
    timeformat 'auto';
    ''' % (table, S3_PATH, ARN_CREDENTIALS, REGION)
    print(copy_command)
    try:
        cur.execute(copy_command)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    ######################################


date = str(sys.argv[1])
domain = str(sys.argv[2])


def scrape_handler():
    today_date = date
    rds.scrape_handler(today_date, domain)
    # return "TESTED"
    for i in list_tbl_files:
      try:
          if not pd.read_csv(S3_FULL_PATH+i+"/"+i+".csv.gz").empty:
            if i=="contains":
              cur.execute("delete from contains where site_id in (select distinct site_id from site where page_url like '%"+domain+"%');")
            if i=="site":
              cur.execute("delete from site where page_url like '%"+domain+"%';")
            if i=="site_category":
              cur.execute("delete from site_category where domain = '"+domain+"';")
          main_handler(i)
      except Exception as e:
          print(str(e)+str(i))
          pass
    #"""
    print("\n\n")
    print("All Tables Copied in S3 and RedShift(data_mart)")
    print("\n\n")

scrape_handler()

