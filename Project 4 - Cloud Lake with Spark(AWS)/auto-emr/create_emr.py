# https://stackoverflow.com/questions/26314316/how-to-launch-and-configure-an-emr-cluster-using-boto
# First run this get the cluster-id and fill and run create_emr-jobstep.py

# Encountered below error ( No time to resolve it ) - Automation stops here
'''
[hadoop@ip-172-31-39-242 ~]$ /usr/local/bin/spark-submit --master yarn ./etl-emrB.py > etl-emrB.txt
Exception in thread "main" org.apache.spark.SparkException: When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.
        at org.apache.spark.deploy.SparkSubmitArguments.error(SparkSubmitArguments.scala:657)
        at org.apache.spark.deploy.SparkSubmitArguments.validateSubmitArguments(SparkSubmitArguments.scala:290)
        at org.apache.spark.deploy.SparkSubmitArguments.validateArguments(SparkSubmitArguments.scala:251)
        at org.apache.spark.deploy.SparkSubmitArguments.<init>(SparkSubmitArguments.scala:120)
        at org.apache.spark.deploy.SparkSubmit$$anon$2$$anon$1.<init>(SparkSubmit.scala:907)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.parseArguments(SparkSubmit.scala:907)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:81)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:920)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:929)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
'''
import boto3
import os
import glob
import json

import configparser
import pandas as pd
from time import time      

def main():
    config = configparser.ConfigParser()
    config.read_file(open('aws/credentials.cfg'))
    KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')

    client = boto3.client('emr', region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                             )

    ClusterName = "Boto3 test cluster 4"
    logUri = 's3://aws-logs-boto3-test4/elasticmapreduce/'
    bootstrapActions = [{
        'Name': 'Install Libraries for etl.py',
        'ScriptBootstrapAction': {
            'Path': 's3://sushanth-dend-datalake-programs/install-libr.sh'
        }
    }]
    
    response = client.run_job_flow(
        Name=ClusterName,
        LogUri=logUri,
        ReleaseLabel='emr-5.20.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 3,
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-1d80e736',
            'Ec2KeyName': 'spark-cluster',
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        BootstrapActions=bootstrapActions
    )
    print('Response : ',response)
    print('Cluster ID : ',response['JobFlowId'])

    
if __name__ == "__main__":
    main()
