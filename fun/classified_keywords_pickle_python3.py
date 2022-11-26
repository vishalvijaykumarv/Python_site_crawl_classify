#!/usr/bin/env python3

import pickle
import boto3
import pandas as pd
import re
import csv
import classified_get_keywords as get_keywords
#s3 = boto3.client('s3', region_name= 'us-east-1')
#s3_bucket = 'data-lake'

clean = re.compile('<.*?>')

data = map(str.lower, pd.read_csv('/tmp/keywords.txt', delimiter=',', dtype='unicode', escapechar='#').columns)
print("start creating pickle for keywords.txt file")
filename = '/opt/url_classification/classified_url/k.txt'
with open(filename, 'wb') as filehandle:
    # store the data as binary data stream
    pickle.dump(data, filehandle)
#print (data)
data = pickle.load(open(filename,'rb'))
ll = list(data)
print(ll)
print(len(ll))
print("Pickle file created as '"+filename+"'")
