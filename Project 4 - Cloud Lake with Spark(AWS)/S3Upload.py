""" 
    Summary line. 
    S3 Upload Utility Program
    Handles Bucket Create, Files & Folders upload
"""

import sys
import os
import glob
import json

import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time      

def upload_files_to_s3(s3c, bucket_name, files):    
    """
    Summary line. 
    Upload array of file using relative path to S3 Bucket
  
    Parameters: 
    arg1 (S3 Client Object)
    arg2 (Bucket name)
    arg3 (array of file names)
  
    Returns: None
    """    
    
    for file in files:
        filename = file.split('/')[-1]
        print('File : {} to {}/{}'.format(file, bucket_name, filename))
        with open(file, 'rb') as data:            
            s3c.put_object(Bucket=bucket_name, Key=filename, Body=data)
             
            
def main():
    config = configparser.ConfigParser()
    config.read_file(open('aws/credentials.cfg'))
    KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')

    s3c = boto3.client('s3', region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    
    location = {'LocationConstraint': 'us-west-2'}    
    bucket_name = 'sushanth-dend-datalake-files'

    try:
        s3c.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
        #Below code not working. AWS came with a brilliant idea of making buckets public by default. 
        #bucket = s3r.bucket(bucket_name)
        #bucket.Acl().put(ACL='private')        
    except:
        print('Bucket Exists')   

    files = ['install-libr.sh', 'etl-emr-hdfs.py', 'etl.py', 'merged_log_data.json', 'merged_song_data.json']
    files = ['Data Lake.zip']
    upload_files_to_s3(s3c, bucket_name, files)
        
    print('Done!')
    
if __name__ == "__main__":
    main()
