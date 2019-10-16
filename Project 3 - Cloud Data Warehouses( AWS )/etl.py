import configparser
import psycopg2
import sql_queries

from importlib import reload
reload(sql_queries)
from sql_queries import schema_queries, copy_table_queries, insert_table_queries, count_rows_queries

import boto3

def load_staging_tables(cur, conn):
    """
    Summary line. 
    Load the data from the S3 bucket to the staging table in Redshift
  
    Parameters: 
    arg1 : cursor connection object on redshift
    arg2 : connection object on the redshift
  
    Returns: 
    None
    """    
    
    print('\r{:5}* {}'.format('',schema_queries[2]['message']))    
    cur.execute(schema_queries[2]['query'])
    
    for o in copy_table_queries:
        print('\r{:5}* {}'.format('',o['message']))    
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()

def insert_tables(cur, conn):
    """
    Insert data from the staging table to the dimension and fact table
    :param cur: cursor connexion object on redshift
    :param conn: connection object on the redshift
    """
    print('\r{:5}* {}'.format('',schema_queries[2]['message']))    
    cur.execute(schema_queries[2]['query'])
    
    for o in insert_table_queries:
        print('\r{:5}* {}'.format('',o['message']))    
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()
            
def count_rows(cur, conn):
    """
    Summary line. 
    Get the number of rows stored into each table
  
    Parameters: 
    arg1 : cursor connection object on redshift
    arg2 : connection object on the redshift
  
    Returns: 
    None
    """    
    
    print('\r{:5}* {}'.format('',schema_queries[2]['message']))    
    cur.execute(schema_queries[2]['query'])
    
    for o in count_rows_queries:
        print('\r{:5}* {}'.format('',o['message']))    
        cur.execute(o['query'])
        #results = cur.fetchone()
        results = cur.fetchall()

        for row in results:
            if(len(row) == 1):
                print('{:8} {}'.format(' ', row[0]))
            else:
                print('{:8} {} - {}'.format(' ', row[0], row[1]))
            #print("   ", type(row), row)
            
def main():
    config = configparser.ConfigParser()
    #config.read('dwh.cfg')

    # Upload cleaned files to s3    
    config.read_file(open('dwh.cfg'))
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')
    
    LOG_LOCAL_DATA = config.get('LOCAL','LOG_LOCAL_DATA')
    SONG_LOCAL_DATA = config.get('LOCAL','SONG_LOCAL_DATA')
    bucket_name = config.get('S3','BUCKET')
    
    #print('LOG_LOCAL_DATA = ',LOG_LOCAL_DATA)
    #print('SONG_LOCAL_DATA = ',SONG_LOCAL_DATA)
    #print('Bucket = ',bucket_name)
    #print( *config['CLUSTER'].values() )

    s3 = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    try:
        location = {'LocationConstraint': 'us-west-2'}        
        print('1. Creating new S3 Bucket : ', bucket_name)
        s3.create_bucket(Bucket=bucket_name,
                         CreateBucketConfiguration=location)
    except Exception as e:
        print('\r{:5}Exception in creating s3 bucket : {}'.format('',e))    
        
    
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.    
    print('2. Uploading file {} to S3'.format(LOG_LOCAL_DATA))
    s3.upload_file(LOG_LOCAL_DATA, bucket_name, LOG_LOCAL_DATA)
    print('3. Uploading file {} to S3'.format(SONG_LOCAL_DATA))
    s3.upload_file(SONG_LOCAL_DATA, bucket_name, SONG_LOCAL_DATA)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()        

    print('4. Loading Staging tables')
    load_staging_tables(cur, conn)
    print('5. Insert into Fact & Dimension tables')
    insert_tables(cur, conn)

    print('6. Count Rows')
    count_rows(cur, conn)
    
    conn.close()
    print('Done!')
    

if __name__ == "__main__":
    main()