#!/bin/bash
set -x -e

#Installing required libraries
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then

sudo pip install pyspark
sudo pip install boto3
sudo pip install configparser
sudo pip install pandas

fi