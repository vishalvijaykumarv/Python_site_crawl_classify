import boto3
import os
from multiprocessing import Pool
import subprocess
import math
import fun.classified_dbredshift as dbredshift
import sys

cwd = os.getcwd()
s3 = boto3.client('s3', region_name='us-east-1')
dates = str(sys.argv[1])  # str(datetime.datetime.now().date())
related_site = str(sys.argv[2]).lower()
s3_bucket = 'data-lake'
Key = "Classified_Url/" + dates + "/"


def main_handler(path):
    print(path)

    sub = subprocess.Popen(["python3", f"{cwd}/scripts/classified_main_caller.py", path, related_site], stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    output, error = sub.communicate()
    if output:
        print("ReturnCode> ", sub.returncode)
        print("OK> output ", output)
        fileurl = output.split()[0].decode('utf-8')
        print(fileurl)
        print(dbredshift.lambda_handler(dates, fileurl))
    if error:
        print("ReturnCode> ", sub.returncode)
        print("Error> error ", error.strip())


def getting_file_paths():
    global number_of_files
    paths = []
    # Getting PATH of Data_Files
    req = s3.list_objects(Bucket=s3_bucket, Prefix=Key)  # 3 columns from glue processing
    for content in req['Contents']:
        if content['Key'][-1] == "z" and content['Key'][-2] == "g" and content['Key'][-3] == ".":
            s3.get_object(Bucket=s3_bucket, Key=content['Key'])
            paths.append("s3://data-lake/" + content['Key'])
    return paths


if __name__ == '__main__':
    # Get the urls from S3
    path = getting_file_paths()  # filepath[:] #number of file path
    print(path)

    # Scrape the all URLs
    path = path[:]  # number of file path
    lenpath = len(path)
    print(lenpath)

    # global chunksize
    chunk_size = 5  # number of chunks in file path (for same file)
    if chunk_size >= lenpath:
        chunk_size = lenpath

    chunks = int(math.ceil(lenpath / chunk_size))  # number of chunks in file path (for same file)
    print(chunks)
    p = Pool(lenpath)  # number of multiprocess #MULTIPROCESS WILL BE THE SAME AS NUMBER OF CHUNKS
    records = p.map(main_handler, path, chunks)  # call multiprocess function
    p.terminate()
