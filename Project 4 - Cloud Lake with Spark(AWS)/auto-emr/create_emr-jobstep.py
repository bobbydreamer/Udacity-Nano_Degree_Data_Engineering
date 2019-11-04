# https://stackoverflow.com/questions/26314316/how-to-launch-and-configure-an-emr-cluster-using-boto

# Fill the cluster-id and run the program
# create_emr.py can be run to create the cluster

import boto3
import os
import glob
import json
import configparser
import pandas as pd

def main():
    config = configparser.ConfigParser()
    config.read_file(open('aws/credentials.cfg'))
    KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
    client = boto3.client('emr', region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                             )

    response = client.add_job_flow_steps(
        JobFlowId='j-3HK52E2ZUEX1H',
        '''
        Steps=[
            {
                'Name': 'Run Step',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--py-files',
                        's3://sushanth-dend-datalake-programs/etl-emrB.py'
                    ],
                    'Jar': 'command-runner.jar'
                }
            }
        ]
        '''
        Steps=[
            {
                'Name': 'Run Step',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Args': [
                        'spark-submit',
                        '--master', 'yarn',
                        '--py-files',
                        's3://sushanth-dend-datalake-programs/etl-emrB.py'
                    ],
                    'Jar': 'command-runner.jar'
                }
            }
        ]
        
    )

    
if __name__ == "__main__":
    main()
