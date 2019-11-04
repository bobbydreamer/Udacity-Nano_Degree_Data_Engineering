# This program is to test in EMR whether a python can create S3 Bucket & Upload file.
# Since its run from EMR KEY & SECRET is not required as you will be executing it via SUDO

import os
import glob
import json

import boto3
import configparser

def main():
    print('file-write started')
    
    f= open("sushanth.txt","w+")
    for i in range(10):
         f.write("This is line %d\r\n" % (i+1))
    f.close() 
    '''
    config = configparser.ConfigParser()
    config.read_file(open('aws/credentials.cfg'))
    KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
    s3c = boto3.client('s3',
                      region_name="us-west-2",
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET
                     )
    '''
    s3c = boto3.client('s3', region_name="us-west-2")
    
    filename = 'file-write.py'
    bucket_name = 'sushanth-dend-datalake-programs'

    filename = 'sushanth.txt'
    bucket_name = 'sushanth-datalaketest-uploads'
        
    location = {'LocationConstraint': 'us-west-2'}
    try:
        s3c.create_bucket(Bucket=bucket_name,
                      CreateBucketConfiguration=location)
    except:
        print('Bucket Exists')
        
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.    
    s3c.upload_file(filename, bucket_name, filename)            
    print('file-write ended')
    
if __name__ == "__main__":
    main()
