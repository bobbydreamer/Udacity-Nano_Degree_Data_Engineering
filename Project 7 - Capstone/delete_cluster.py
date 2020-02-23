import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser

from random import random
import threading
import time

# Tracking Cluster Creation Progress
progress = 0
cluster_status = ''
cluster_event = threading.Event()

def initialize():
    """
    Summary line. 
    This function starts the delete_cluster function. 
  
    Parameters: 
    NONE
  
    Returns: 
    None
    """    
    
    # Reading the configuration file
    config = configparser.ConfigParser()
    config.read_file(open('./aws/aws-capstone.cfg'))
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("CLUSTER","DWH_DB")
    DWH_DB_USER            = config.get("CLUSTER","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

    bucket_name            = config.get("S3", "BUCKET")

    df = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })

    print(df)


    # Initializing the AWS resources
    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    
    myClusterProps = get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER)
    
    if myClusterProps[0] != '':
        #print(myClusterProps)
        prettyRedshiftProps(myClusterProps[0])
        DWH_ENDPOINT = myClusterProps[1]
        DWH_ROLE_ARN = myClusterProps[2]
        #print('Test DWH_ROLE_ARN : ',DWH_ROLE_ARN)
    
    print('1. Delete Redshift Cluster')
    delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER)

    thread = threading.Thread(target=lambda : check_cluster_status(redshift, DWH_CLUSTER_IDENTIFIER, 'delete', 'none'))
    thread.start()

    # wait here for the result to be available before continuing
    while not cluster_event.wait(timeout=5):
        print('\r{:5}Waited for {} seconds. Redshift Cluster Deletion in-progress...'.format('', progress), end='', flush=True)
        
    print('\r{:5}Cluster deletion completed. Took {} seconds.'.format('', progress))    

    
    delete_iam_role(iam, DWH_IAM_ROLE_NAME)
    

    print('Please go and manually delete the bucket')
    #print('4. Deleting S3 Bucket')
    #bucket = s3.Bucket(bucket_name)
    
    # suggested by Jordon Philips 
    #bucket.objects.all().delete()    
    #bucket.delete()
    
    print('Done!')
    
    
def delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Summary line. 
    Deletes Redshift Cluster
  
    Parameters: 
    arg1 : Redshift Object
    arg2 : Cluster Name
  
    Returns: 
    None
    """        
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)    
    
def prettyRedshiftProps(props):
    """
    Summary line. 
    Returns the Redshift Cluster Properties in a dataframe
  
    Parameters: 
    arg1 : Redshift Properties
  
    Returns: 
    dataframe with column key, value
    """        
    
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def check_cluster_status(redshift, DWH_CLUSTER_IDENTIFIER, action, status):
    """
    Summary line. 
    Check the cluster status in a loop till it becomes available/none. 
    Once the desired status is set, updates the threading event variable
  
    Parameters: 
    arg1 : Redshift Object
    arg2 : Cluster Name
    arg3 : action which can be (create or delete)
    arg4 : status value to check 
    
    Returns: 
    NONE
    """        
    
    global progress
    global cluster_status

    # wait here for the result to be available before continuing        
    while cluster_status.lower() != status:
        time.sleep(5)
        progress+=5
        if action == 'create':
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            #print(myClusterProps)
            df = prettyRedshiftProps(myClusterProps)
            #print(df)
            #In keysToShow 2 is ClusterStatus
            cluster_status = df.at[2, 'Value']            
        elif action =='delete':
            myClusterProps = redshift.describe_clusters()
            #print(myClusterProps)
            if len(myClusterProps['Clusters']) == 0 :
                cluster_status = 'none'
            else:
                myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
                #print(myClusterProps)
                df = prettyRedshiftProps(myClusterProps)
                #print(df)
                #In keysToShow 2 is ClusterStatus
                cluster_status = df.at[2, 'Value']                            

        print('Cluster Status = ',cluster_status)
                
    # when the calculation is done, the result is stored in a global variable
    cluster_event.set()

    # Thats it


def delete_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Summary line. 
    Delete IAM Role that allows Redshift clusters to call AWS services on your behalf
  
    Parameters: 
    arg1 : IAM Object
    arg2 : IAM Role name
  
    Returns: 
    NONE
    """        
    
    try:
        print('2. Detach Policy')
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print('3. Delete Role : {}'.format(DWH_IAM_ROLE_NAME))
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(e)
        
def get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Summary line. 
    Retrieve Redshift clusters properties
  
    Parameters: 
    arg1 : Redshift Object
    arg2 : Cluster Name
  
    Returns: 
    myClusterProps=Cluster Properties, DWH_ENDPOINT=Host URL, DWH_ROLE_ARN=Role Amazon Resource Name
    """        
    
    myClusterProps = redshift.describe_clusters()
    #print(myClusterProps)
    if len(myClusterProps['Clusters']) == 0 :
        #print('Clusters = 0')
        return '', '', ''
    else:
        #print('Clusters != 0')
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        #print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        #print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
        return myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN

def main():
    
    initialize()

if __name__ == "__main__":
    main()
