import boto3,psycopg2,sys, ast,re,datetime,os,math,glob,shutil,gc,pymysql,configparser
from multiprocessing import Pool
from datetime import timedelta
from pandas import read_sql
import pandas as pd

# Display Environment Settings:
pd.set_option('display.max_column', None)
pd.set_option('display.max_seq_items', None)
pd.set_option('display.max_colwidth', 50)
pd.set_option('expand_frame_repr', False)

cwd = os.getcwd()
dd = sys.argv[1]
domain = sys.argv[2]
cust_segment_ids = ast.literal_eval(sys.argv[3])

previous_date = str(datetime.datetime.strptime(dd, '%Y-%m-%d').date() - timedelta(days=1))           # str(datetime.datetime.now().date()  - timedelta(days=90))
today_date = dd                                                                                      # str(datetime.datetime.now().date())
s3 = boto3.resource('s3')
Key = "Segment_Data/Classified_data/" + domain + "/"
s3file = 'bw_cookie_' + dd + '.csv'                                                                 # datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

upload_file_name_qa = f"{cwd}/Data_Files/flag_cust_segment_" + domain.replace('.', '_') + "_qa.csv"
upload_file_name_x_Classify_Name = f"{cwd}/Data_Files/flag_cust_segment_" + domain.replace('.', '_') + "_x_Classify_Name.csv"
file_dir = f"{cwd}/Data_Files/flag_upload/" + today_date + "/" + domain.replace('.', '_')

config = configparser.RawConfigParser()
config.read(f"{cwd}/scripts/config.cfg")
red_shift_config = dict(config.items('REDSHIFT'))
RDS_config = dict(config.items('RDS'))

try:
    rds_conn = pymysql.connect(host=RDS_config['host_add'], user=RDS_config['username'], passwd=RDS_config['password'],
                               db=RDS_config['database'], connect_timeout=5)
    rds_cur = rds_conn.cursor()
except:
    print("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()


def create_conn(*args, **kwargs):
    # config = kwargs['config']
    try:
        con = psycopg2.connect(dbname=red_shift_config['database'], host=red_shift_config['host_add'],
                               port=red_shift_config['host_prt'], user=red_shift_config['username'],
                               password=red_shift_config['password'])
        print("connected to redshift")
        return con
    except Exception as err:
        print(err)


def get_cust_ids():
    con = create_conn()

    segment_ids = "select distinct custom_segment_id as id from segment inner join custom_segments on custom_segments.id = segment.custom_segment_id " \
                  "order by custom_segment_id ASC"  # "select distinct custom_segment_id as id from segment order by custom_segment_id ASC"  #Getting Custom Segment Ids from RedShift
    try:
        print(segment_ids)
        dfff = read_sql(segment_ids, con=con)
        # print(dfff)
        dfff = list(set(dfff['id'].drop_duplicates()))  # List of Custom Segment Ids
        # dfff.sort()
        return dfff
    except Exception as e:
        print("\n\nERROR: " + str(e))
        pass


def get_query_data(custom_segment_id):
    con = create_conn()
    i = custom_segment_id
    query_res = "select id, threshold, name from custom_segments where id=" + str(i)  # Getting Threshold value from RDS
    try:
        # print(threshold)
        rds_cur.execute(query_res)
    except Exception as e:
        print(e)
        rds_conn.rollback()

    try:
        query_res = rds_cur.fetchone()
        # cs_name=rds_cur.fetchone()
        id_res = str(query_res[0])
        threshold_res = str(query_res[1])
        cus_name = str(query_res[2])

        cust_segment = "Query for Taking the custome segment data from the redshift"
        print(cust_segment)
        cust_segment_res = read_sql(cust_segment, con=con)
        cust_segment_res.dropna(subset=['category'], how='all', inplace=True)
        cust_segment_res.reset_index(drop=True)
        print(cust_segment_res)

        if cust_segment_res.empty == True:
            print("EMPTY DATA for the id: " + str(id_res))
        else:
            textfile = file_dir + "/custom_segment_" + str(id_res) + ".csv"
            file = cust_segment_res.to_csv(textfile, index=False, encoding='utf-8')
            return ("Query has been completed for the Custom Segment Id: " + str(id_res))
        """"""
    except Exception as e:
        print("\nERROR for custom_segment ID " + str(i) + ": " + str(e))
        pass


def concate_files_qa():
    lst_files = glob.glob(file_dir + "/*")  # Get all files from directory
    df_from_each_file = (pd.read_csv(f) for f in lst_files)
    concatenated_df = pd.concat(df_from_each_file, ignore_index=True, sort=False).dropna()

    # Remove Duplicate segment_id from Each Rows
    concatenated_df['category'] = concatenated_df['category'].astype(str).dropna()
    concatenated_df['listpoints'] = concatenated_df['listpoints'].astype(str).dropna()
    concatenated_df['sumofpoints'] = concatenated_df['sumofpoints'].astype(str).dropna()

    # For QA All Cols
    liveramp_file = concatenated_df.groupby(['url', 'category'])['keyword', 'flag', 'listpoints', 'sumofpoints'].agg((' | '.join)).reset_index()

    # If >=3 keywords
    liveramp_file['numberofkeywords'] = liveramp_file.keyword.str.split('|', expand=False).agg([len]).astype(int).dropna()

    liveramp_file.loc[liveramp_file['category'].str.contains('HCP'), 'Value Type'] = 'HCP'
    liveramp_file.loc[liveramp_file['category'].str.contains('DTC'), 'Value Type'] = 'DTC'
    print(liveramp_file)

    liveramp_file['Value Type2'] = liveramp_file.apply(lambda x: 'True' if x['Value Type'] == 'HCP' and x['numberofkeywords'] >= 5 else (
        'True' if x['Value Type'] == 'DTC' and x['numberofkeywords'] >= 3 else 'False'), axis=1)

    liveramp_file = liveramp_file[liveramp_file['Value Type2'] == 'True']
    liveramp_file.drop(['Value Type', 'Value Type2'], axis=1, inplace=True)
    print(liveramp_file)

    """"""
    liveramp_file.to_csv(upload_file_name_qa, sep=',', header=True, index=False, encoding='utf-8')  # Create file with Seperator ","
    # Remove all single/double quotes from file
    with open(upload_file_name_qa, 'r+') as f:
        data = f.read()
        f.seek(0)
        f.write(re.sub(r'"', '', data))
        f.truncate()
    """"""
    print('QA Local File created at: ' + upload_file_name_qa)


def concate_files_x_Classify_Name():
    concatenated_df = pd.read_csv(upload_file_name_qa, usecols=['url', 'category'])
    # Remove Duplicate segment_id from Each Rows
    concatenated_df['category'] = concatenated_df['category'].astype(str).dropna()
    # For X.Classify_Namemedia.com Only 2 Cols
    liveramp_file = concatenated_df.groupby('url')['category'].agg([('category', ' | '.join)]).reset_index()  # .sort_values(['url'], ascending=[True])
    print(concatenated_df)
    print(liveramp_file)

    """"""
    liveramp_file.to_csv(upload_file_name_x_Classify_Name, sep=',', header=True, index=False, encoding='utf-8')  # Create file with Seperator ","
    # Remove all single/double quotes from file
    with open(upload_file_name_x_Classify_Name, 'r+') as f:
        data = f.read()
        f.seek(0)
        f.write(re.sub(r'"', '', data))
        f.truncate()
    """"""
    print('x.Classify_Name.com Local File created at: ' + upload_file_name_x_Classify_Name)


def upload_to_s3():
    s3.Bucket('data-lake').upload_file(Filename=upload_file_name_qa, Key=Key + domain.replace('.', '_') + "_qa.csv", ExtraArgs={'ACL': 'public-read'})
    print("\nQA File uploaded to https://data-lake.s3.amazonaws.com/" + Key + domain.replace('.', '_') + "_qa.csv \n")
    s3.Bucket('data-lake').upload_file(Filename=upload_file_name_x_Classify_Name, Key=Key + domain.replace('.', '_') + "_x_Classify_Name.csv",
                                               ExtraArgs={'ACL': 'bucket-owner-full-control'})
    print("x.Classify_Name.com File uploaded to https://data-lake.s3.amazonaws.com/" + Key + domain.replace('.', '_') + "_x_Classify_Name.csv \n")


if __name__ == '__main__':
    """"""
    print("\n\n===================================================================================================================================\n\n")
    print("Day of Data: " + today_date)
    print("\n\n===================================================================================================================================\n\n")
    try:
        # Remove 7 Days Older Query Directory
        shutil.rmtree(f"{cwd}/Data_Files/flag_upload/" + str(datetime.datetime.strptime(dd, '%Y-%m-%d').date() - timedelta(days=7)), ignore_errors=True, onerror=None)
        # Create New Directory for latest Query Results
        os.makedirs(file_dir)
    except Exception as e:
        os.listdir(file_dir)
        pass
    if len(cust_segment_ids) == 0:
        cust_segment_ids = get_cust_ids()
    ids = cust_segment_ids[:]  # number of file path
    print(ids)
    path_length = len(ids)
    print("Days: " + str(path_length))
    # global chunk_size
    chunk_size = 10
    chunks = int(math.ceil(path_length / chunk_size))  # number of chunks
    print("chunk_size: " + str(chunks))

    p = Pool(path_length)  # number of multiprocess #MULTIPROCESS WILL BE THE SAME AS NUMBER OF CHUNKS
    records = p.map(get_query_data, ids, chunks)  # call multiprocess function
    print(records)
    p.terminate()
    gc.collect()
    print("Query Completed")

    # Concatinate_files
    concate_files_qa()

    concate_files_x_Classify_Name()
    gc.collect()

    # Upload file to S3 bucket ( it may process the data )
    upload_to_s3()
