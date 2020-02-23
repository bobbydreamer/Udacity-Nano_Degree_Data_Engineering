import configparser
import psycopg2
import json
import sql_queries

from importlib import reload
reload(sql_queries)
from sql_queries import schema_queries, insert_table_queries

import boto3


def get_aws_keysecret(filepath):
    with open(filepath) as myfile:
        head = [next(myfile) for x in range(4)]
    #print(head)
    #print(head[3])
    for s in head:
        #print("KEYSECRET" in s)
        if "KEYSECRET" in s:
            arr = s.split('KEYSECRET=')
            key = arr[1]
            #print(key)
            break
    return key.strip()

def make_copy_statement(table, data, awskey):
    copy_stmt = ("""
        COPY {} FROM {}
        CREDENTIALS '{}'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        CSV 
        delimiter ',' 
        IGNOREHEADER 1
        COMPUPDATE OFF REGION 'us-west-2';
        """).format(table, data, awskey)
    return copy_stmt

def load_staging_i94_table(cur, conn, bucket_name):
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
        
    table = 'staging_ids'
    IAM_ROLE = 'arn:aws:iam::164084742828:role/dwhRole'
    
    #Parquet Issues : Null column are not written to output
    #copy_stmt = "COPY {} FROM '{}' IAM_ROLE '{}' FORMAT AS PARQUET ;".format(table, "s3://sushanth-dend-capstone-files/i94-apr16.parquet/", IAM_ROLE)

    copy_stmt = "COPY {} FROM '{}' IAM_ROLE '{}' csv gzip IGNOREHEADER 1 region 'us-west-2' ;".format(table, bucket_name, IAM_ROLE)
    print(copy_stmt)
    #print('Loading {} with data from {}'.format(table, data))    
    try:
        cur.execute(copy_stmt)
        conn.commit()
    except psycopg2.Error as e:
        print(e)
        conn.close()        


def load_staging_tables(cur, conn, KEYSECRET, fname):
    """
    Summary line. 
    Load the data from the S3 bucket to the staging table in Redshift
  
    Parameters: 
    arg1 : cursor connection object on redshift
    arg2 : connection object on the redshift
  
    Returns: 
    None
    """    

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
    """            
    print('\r{:5}* {}'.format('',schema_queries[2]['message']))    
    cur.execute(schema_queries[2]['query'])
    
    table_data = json.load(open(fname))
    for table, data in table_data.items():        
        if(data == 'ignore'):
            continue            
        
        print('Loading {} with data from {}'.format(table, data))
        copy_stmt = make_copy_statement(table, data, KEYSECRET)
        try:
            cur.execute(copy_stmt)
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

def count_rows_staging(cur, conn, fname):
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
    
    table_data = json.load(open(fname))
    for table, data in table_data.items():
        #print(table, data)    
        sql_stmt = 'SELECT COUNT(*) FROM {}'.format(table)
        try:
            cur.execute(sql_stmt)
            results = cur.fetchall()
            for row in results:
                #print("   ", type(row), row)                            
                if(len(row) == 1):
                    print('{:8} {:25} - {}'.format(' ', table, row[0]))
        except psycopg2.Error as e:
            print(e)
            conn.close()        
                        
def main():
    config = configparser.ConfigParser()
    #config.read('dwh.cfg')

    # Upload cleaned files to s3    
    config.read_file(open('./aws/aws-capstone.cfg'))
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')
    KEYSECRET= config.get('AWS','keysecret')
    
    awskey = get_aws_keysecret("./aws/aws-capstone.cfg")
    
    #print('LOG_LOCAL_DATA = ',LOG_LOCAL_DATA)
    #print('SONG_LOCAL_DATA = ',SONG_LOCAL_DATA)
    #print('Bucket = ',bucket_name)
    #print( *config['CLUSTER'].values() )

    s3 = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()        
        
    print('1. Loading Staging tables')
    fname = 'inputs/staging-table-data.txt'
    load_staging_tables(cur, conn, KEYSECRET, fname)
    
    print('2. Loading Staging i94 table')
    load_staging_i94_table(cur, conn, "s3://sushanth-dend-capstone-files2/dfs_ids1.gzip")
    
    print('3. Insert into Fact & Dimension tables')
    insert_tables(cur, conn)

    print('4. Count Rows')
    count_rows_staging(cur, conn, fname)
    
    conn.close()
    print('Done!')
    

if __name__ == "__main__":
    main()