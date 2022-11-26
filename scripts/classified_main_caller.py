import os, re, math, json, datetime, pytz, sys, boto3
import pandas as pd
from multiprocessing import Pool
from multiprocessing import cpu_count
import subprocess


cwd = os.getcwd()
s3 = boto3.client('s3')
FNULL = open(os.devnull, 'w')
fileurl = str(sys.argv[1].replace("'", ""))  # os.environ.get('FILE_PATH')#sys.argv[1] #GET THE FILE URL/PATH
related_site = str(sys.argv[2])
n_chunk = int(cpu_count() * 1)  # int(sys.argv[2]) #GET THE NUMBER OF CHUNKS FOR URLS
fields = ['page_url']


def scrape_handler(url):
    if related_site == 'health':
        sub = subprocess.Popen(["timeout", "120", "python3", f"{cwd}/scripts/classified_health_Contextual_Scrapper.py", url],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(sub.communicate())
    elif related_site == "non-health":
        sub = subprocess.Popen(["timeout", "120", "python3", f"{cwd}/scripts/classified_non_health_Contextual_Scrapper.py", url],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(sub.communicate())
    else:
        print("Please add Site Relatability (HEALTH/NON-HEALTH)!!")


if __name__ == '__main__':
    # GETTING URLS FROM FILE
    print(fileurl)
    df = pd.read_csv(fileurl, usecols=fields, compression='gzip', header=0, sep=',', quotechar='"', delimiter=',', escapechar='\\', dtype='unicode',
                     error_bad_lines=False, index_col=False)
    print(df)
    df.dropna(subset=['page_url'], inplace=True)
    df.drop_duplicates(keep='first', inplace=True)
    df.reset_index(drop=True)
    urlval = df.loc[:, 'page_url']
    lendff = len(urlval)
    print("\n" + str(lendff) + ' URLs in ' + fileurl + "\n")
    chunks = int(math.ceil(lendff / int(n_chunk)))  # NUMBER OF CHUNKS OF URL
    print(chunks)
    p = Pool(n_chunk)  # number of multiprocess #MULTIPROCESS WILL BE THE SAME AS NUMBER OF CHUNKS
    records = p.map(scrape_handler, urlval, chunks)  # call multiprocess function
    p.terminate()
