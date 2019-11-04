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

    s3c = boto3.client('s3', 
                               region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                             )
    
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
