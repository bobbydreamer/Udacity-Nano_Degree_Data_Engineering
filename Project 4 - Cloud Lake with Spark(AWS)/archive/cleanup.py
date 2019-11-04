""" 
    Summary line. 
    S3 Bucket Creation & File Upload Test Program
"""

import os
import glob
import json

import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time      


def upload_files(s3, bucket_name, path):
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
                bucket.put_object(Key=full_path[len(path)+1:], Body=data)
                
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
    
    '''
    s3 = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )

    path = './output/artists.parquet'
    bucket_name = 'datalake-outputs123'

    location = {'LocationConstraint': 'us-west-2'}
    try:
        s3.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
    except:
        print('Bucket Exists')
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.    
    #s3.upload_file(filename, bucket_name, filename)            
    
    
    
    s3r = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    bucket = s3r.Bucket(bucket_name)
    
    upload_files(s3r, bucket_name, path)
    '''
    
    print('AWS S3 Bucket Cleanup')
    s3r = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    
    bucket_names = ['sushanth-dend-datalake-parquet-files3']
    for buck in bucket_names:
        t0 = time()
        try:
            print('Deleting bucket {}'.format(buck))
            bucket = s3r.Bucket(buck)
            bucket.objects.all().delete()    
            bucket.delete()            
        except Exception as e:
            print('Failed deleting bucket {}'.format(buck))
            print('Message : {}'.format(e))
            
        uploadTime = time()-t0
        print("=== {} Deleted in: {} sec\n".format(buck, round(uploadTime,2) ))


        
if __name__ == "__main__":
    main()
