""" 
    Summary line. 
    Downloads files(log_data & song_data) from Udacity Bucket 
    and merges respective files into a single file
"""
import os
import glob
import json

import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time

#Functions
def download_files_from_s3(Bucket, prefix, limit):
    """
    Summary line. 
    Download files from AWS_S3
  
    Parameters: 
    arg1 (prefix of files to be downloaded)
    arg2 (number of files to be downloaded)
  
    Returns: 
    None
    """    
    i = 0
    files = []    
    tenPercentOfTotal = round(limit/10)
    
    for obj in Bucket.objects.filter(Prefix=prefix):
        if(len(files) % tenPercentOfTotal == 0 and i>1):
            print('Downloaded {}/{}'.format(len(files), limit) )
            
        if i>limit:
            break
        if obj.key.rsplit('/', 1)[1] != '':        
            folder = obj.key.rsplit('/', 1)[0]
            file= obj.key.rsplit('/', 1)[1]
            files.append(obj.key)
            #print(obj.key)
            os.makedirs(folder, exist_ok=True)  # succeeds even if directory exists.
            Bucket.download_file(obj.key, obj.key)        
        i+=1
        
def get_all_files(folder):
    """
    Summary line. 
    Scans folder and prepares files list
  
    Parameters: 
    arg1 (folder path)
  
    Returns: 
    Array of filepath
    """     
    # 1. checking your current working directory
    print('Current Working Directory : ',os.getcwd())

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + folder
    print('Scanning Directory : ',filepath)

    # 2. Create a for loop to create a list of files and collect each filepath
    #    join the file path and roots with the subdirectories using glob
    #    get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # 3. get total number of files found
    num_files = len(all_files)
    print('{} files found'.format(num_files))
    #print(all_files)
    return all_files
        
        
def merge_song_files(folder, mergedfile):        
    """
    Summary line. 
    Merges all song data into one file. Each song file contains only one JSON object.
  
    Parameters: 
    arg1 (folder path)
    arg2 (merged output filename)
  
    Returns: 
    None
    """     
    output_list = []
    all_files = get_all_files(folder)
    if len(all_files) > 0:
        for f in all_files:
            with open(f, "rb") as infile:
                output_list.append(json.load(infile))

        with open(mergedfile, "w", encoding="utf8") as outfile:
            json.dump(output_list, outfile)    

            
def merge_log_files(folder, mergedfile):
    """
    Summary line. 
    Merges all log data into one file. Each log file may contain more than one JSON objects and there will be only one JSON object per line.
  
    Parameters: 
    arg1 (folder path)
    arg2 (merged output filename)
  
    Returns: 
    None
    """         
    output_list = []
    all_files = get_all_files(folder)
    if len(all_files) > 0:
        for f in all_files:
            with open(f, 'r') as f:
                for line in f:
                    output_list.append(json.loads(line))

        with open(mergedfile, "w", encoding="utf8") as outfile:
            json.dump(output_list, outfile)    

            
def json_to_dataframe(infile):
    """
    Summary line. 
    Reads JSON file loads panda datafame
  
    Parameters: 
    arg1 (filename)
  
    Returns: 
    dataframe
    """         
    with open(infile) as datafile:
        data = json.load(datafile)

    df = pd.DataFrame(data)
    print("df shape {}".format(df.shape))
    return df
    
    
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')

    DWH_DB= config.get("CLUSTER","DWH_DB")
    DWH_DB_USER= config.get("CLUSTER","DWH_DB_USER")
    DWH_DB_PASSWORD= config.get("CLUSTER","DWH_DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER","DWH_PORT")

    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )

    sampleDbBucket =  s3.Bucket("udacity-dend")
    
    #print('Downloading song_data')
    #download_files_from_s3(sampleDbBucket, 'song_data', 20000)
    
    print('Downloading log_data')
    download_files_from_s3(sampleDbBucket, 'log_data', 50)
    
    print('Downloading log_json_path.json')
    sampleDbBucket.download_file('log_json_path.json', 'log_json_path.json')
    
    print('Merging song_data')
    merge_song_files('/song_data', 'merged_song_data.json')
    
    print('Merging log_data')
    merge_log_files('/log_data', 'merged_log_data.json')
        

if __name__ == "__main__":
    main()
