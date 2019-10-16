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
    
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')

    DWH_DB= config.get("CLUSTER","DWH_DB")
    DWH_DB_USER= config.get("CLUSTER","DWH_DB_USER")
    DWH_DB_PASSWORD= config.get("CLUSTER","DWH_DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER","DWH_PORT")
    '''
    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    '''
    s3 = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )

    filename = 'song_df_clean.csv'
    bucket_name = 'dwh-cleaned-files'

    location = {'LocationConstraint': 'us-west-2'}
    s3.create_bucket(Bucket=bucket_name,
                            CreateBucketConfiguration=location)
    
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.    
    s3.upload_file(filename, bucket_name, filename)            

    s3r = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    bucket = s3r.Bucket(bucket_name)
    # suggested by Jordon Philips 
    bucket.objects.all().delete()    
    bucket.delete()

    
if __name__ == "__main__":
    main()
