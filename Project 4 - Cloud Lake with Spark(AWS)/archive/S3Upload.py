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


def upload_files(s3, bucket_name, folder, path):
    '''
    session = boto3.Session(
        aws_access_key_id='YOUR_AWS_ACCESS_KEY_ID',
        aws_secret_access_key='YOUR_AWS_SECRET_ACCESS_KEY_ID',
        region_name='YOUR_AWS_ACCOUNT_REGION'
    )
    s3 = session.resource('s3')    
    '''
    bucket = s3.Bucket(bucket_name)
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                #print('Fullpath : ',full_path)
                #print(full_path[len(path)+1:])
                #print(folder+'/'+full_path[len(path)+1:])
                #bucket.upload_file('/tmp/' + filename, '<bucket-name>', 'folder/{}'.format(filename))
                bucket.put_object(Key=folder+'/'+full_path[len(path)+1:], Body=data)

def upload_files_to_s3(s3c, bucket_name, files):        
    for file in files:
        filename = file.split('/')[-1]
        print('File : {} to {}/{}'.format(file, bucket_name, filename))
        s3c.put_object(Bucket=bucket_name, Key=filename, Body=file)
                
def main():
    config = configparser.ConfigParser()
    config.read_file(open('aws/credentials.cfg'))
    KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')
    '''
    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    '''
    s3c = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    
    s3r = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    

    location = {'LocationConstraint': 'us-west-2'}    
    bucket_name = 'sushanth-dend-datalake-parquet-files'
    bucket_name = 'sushanth-dend-datalake-files'

    try:
        s3c.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
        #Below code not working. AWS came with a brilliant idea of making buckets public by default. 
        #bucket = s3r.bucket(bucket_name)
        #bucket.Acl().put(ACL='private')        
    except:
        print('Bucket Exists')   

    files = ['install-libr.sh', 'archive/etl-model.py']
    upload_files_to_s3(s3c, bucket_name, files)
        
    '''
    tables = ['artists', 'songplays', 'songs', 'time', 'users']
    #tables = ['artists']
    for table in tables:
        print('Uploading {} files'.format(table))
        t0 = time()
        filepath = './output/'+table+'.parquet'
        s3_subfolder = table
        s3c.put_object(Bucket=bucket_name, Key=(s3_subfolder+'/'))    
        #s3c.put_object(Bucket=bucket_name, Key=(filepath+'/'))    
        
        s3_subfolder = bucket_name+'/'+s3_subfolder
        bucket = s3r.Bucket(s3_subfolder)
        #path = './output/artists.parquet'
        upload_files(s3r, bucket_name, table, filepath)
        
        uploadTime = time()-t0
        print("=== {} Uploaded in: {} sec\n".format(table, round(uploadTime,2) ))
    '''    
                        
    '''
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.    
    # s3.upload_file(filename, bucket_name, filename)                    
    
    s3r = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    bucket = s3r.Bucket(bucket_name)
    path = './output/artists.parquet'
    upload_files(s3r, bucket_name, path)
    '''

    print('Done!')
    
if __name__ == "__main__":
    main()
