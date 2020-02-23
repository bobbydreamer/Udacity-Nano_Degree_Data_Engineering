#  Data Lake with Spark ( AWS )

> ## Udacity Data Engineer Nano Degree Project 4

## Project Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Technical Overview
Project is implemented in AWS Cloud and uses following products, 
1. S3 Storage Buckets
1. [Apache Spark](./spark.md)
1. [EMR Jupyter Notebooks](./EMRJupyterNotebook.md)
1. [AWS Athena](./athena.md)
1. [AWS EMR(Elastic MapReduce)](./emr.md)
1. puTTY
1. puTTYgen
1. SCP

## Goal

* **Primary goal** : Read data from S3 Storage and perform analytical processing in EMR and load the results back to S3. 

* **Test** : Work on given small input dataset given by udacity. ( /data/log-data.zip, /data/song-data.zip)

## Files in the folder

**Python Programs**
* S3Upload.py : Uploads all the necessary python programs to S3 to be used in EMR Cluster
* S3Download.py : Downloads files(log_data & song_data) from Udacity Bucket  

* s3-file-write-from-emr.py : This program is to test in EMR whether a python can create S3 Bucket & Upload file
cleanup.py : Deletes all the S3 Buckets
* etl-local.py : Runs the ETL program locally(input & output in project workspace). 
* etl-emrA.py :  Reads the inputs from project workspace & uploads the output to S3 Buckets. For output [click here](./outputs/etl-emrA.md)
* etl-emr-hdfs.py : Run this program in EMR Spark this is to test Hadoop HDFS. Input files needs to copied to HDFS to run this and outputs will also be created in HDFS filesystem only.
* etl.py : This is the final program. Reads input from S3 Buckets and writes the parquet files to S3 Buckets


**Juputer Notebooks**
* DataLake - Local.ipynb : Data lake tests with .zip file given to Udacity

* DataLake - S3-to-Local.ipynb : This is used GATHER, ASSESS, CLEAN and ANALYTICS. Main Jupyter Notebook.
* DL-Analytics.ipynb : This Notebook file was downloaded from AWS EMR after running etl.py in AWS EMR Cluster and writing all the parquet files to S3. This is the final result.

**Other files**
* install-libr.sh : This file is uploaded to S3 and will be downloaded to EMR to install some python packages before running the etl.py. For Command output [click here](./outputs/install-libr-output.md)

## How to run

### Running Local  

**Program** : etl-local.py  

I have downloaded all the files from S3 and saved it locally in the project workspace.
* s3://udacity-dend/song_data
* s3://udacity-dend/log_data

Input files are available in 
```
./song_data/*/*/*/*.json
./log_data/*/*/*.json
```

Output parquet files come in folder ./output/

Execute the below commands to run the file locally,
```
rm -r ./output/
%run ./etl-local.py
```

### Running in AWS

**Program** : etl-emr-hdfs.py  

This is the next upgrade to the program, should be executed in  EMR Cluster. 
Below are the input files which is just the merged version of the JSON files from the S3 bucket. Files are merged as its easier to transfer a single file than a folder of files.
```
./merged_log_data.json
./merged_song_data.json
```

Output will be created in the Hadoop HDFS filesystem.

Below are the steps,

1. Run S3Upload.py as this upload all the necessary files to S3 Bucket which will be used in the EMR Cluster
```
%run ./S3Upload.py
``` 
2. Log into EMR Cluster. You can run "pip freeze" to know python packages already installed in the node.  
3. Run below command to download files from S3 to hadoop user home folder
```aws s3 sync s3://sushanth-dend-datalake-files . ```
4. Give execute access to install-libr.sh  
```chmod 755 install-libr.sh```
5. Execute install-libr.sh  
```./install-libr.sh```
6. Copy merged_log_data.json & merged_song_data.json files to HDFS filesystem. Here is user is 'root' because sudo is used to submit the spark job.
```
hadoop fs -ls /user/root

hadoop fs -put ./merged_log_data.json /user/root/merged_log_data.json
hadoop fs -put ./merged_song_data.json /user/root/merged_song_data.json

hadoop fs -ls /user/root
```

7. Spark submit the python program to read file from HDFS and perform ETL to S3. You can use ( ```which spark-submit``` ) to know the folder where the spark is installed.  
```sudo /usr/bin/spark-submit --master yarn ./etl-emr-hdfs.py > etl-emr-hdfs.txt```

Once the job completes, verify using below command
```
hadoop fs -ls /user/root/output2
```

For job outputs, [click here](./outputs/etl-emr-hdfs.md)

**Program** : etl.py  

This is the next & final upgrade to the program and should be executed in EMR Cluster. This program reads directly from S3 and processes the data in EMR Spark Cluster and writes the outputs directly to S3 Bucket.

Input
```
s3://udacity-dend/song_data
s3://udacity-dend/log_data
```

Output : ``` s3://sushanth-dend-datalake-parquet-files3 ```

Below are the steps,
If you haven't tried previous section(Testing HDFS in EMR), you need to do the following to download files from S3

1. Run S3Upload.py as this upload all the necessary files to S3 Bucket which will be used in the EMR Cluster
```
%run ./S3Upload.py
``` 
2. Log into EMR Cluster
3. Run below command to download files from S3 to hadoop user home folder  
```aws s3 sync s3://sushanth-dend-datalake-files . ```
4. Give execute access to install-libr.sh  
```chmod 755 install-libr.sh```
5. Execute install-libr.sh  
```./install-libr.sh```
6. Spark submit the python program to read file from HDFS and perform ETL to S3  
```sudo /usr/bin/spark-submit --master yarn ./etl.py > etl.txt```  

Once the job completes, verify S3 Bucket : ```sushanth-dend-datalake-parquet-files3```

For job output [click here](./outputs/etl.md)

### **Parquet files can queried from AwS Athena like a relational table.**
* [Setting up AWS Athena : Step-by-Step](./athena.md)

## Project Progression Step-by-Step

Udacity had provided a sample of the input files(song & log) in a zip format, so initially assumed in S3 also it will be zipped. (Wrong Assumption)

Used Jupyter Notebook for inital analysis & code
### **Gather**
* Unzipped the song & log files
* Read log file using ```spark.read.json()``` - No issues
* Had issues in reading song datasets, so had provide a schema and then read like ```song_df = spark.read.json(path, schema=song_schema)```

### **Assess**
* Successfully assessed datasets and found below Quality & Tidiness issues

**Quality**
1. song_df : Replace artist_latitude & artist_longitude null with 0  

**Tidiness**  
1. log_df : Filter only column page with value "NextSong"
2. log_df : Convert ts from long to datetime
3. log_df : Convert registration from double to long
4. log_df : Create user level analysis table
5. Create below Fact & Dimension tables
    * create songplays table with columns songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    * create users table with columns user_id, first_name, last_name, gender, level
    * create songs table with columns song_id, title, artist_id, year, duration
    * create artists table with columns artist_id, name, location, lattitude, longitude
    * create time table with columns start_time, hour, day, week, month, year, weekday

### **Clean**
Handled all the above Quality & Tidiness issues

## Data Modeling (Panda dataframes)  
Table design for SongPlay Analysis via Star Schema  

### Fact Table
**songplays** : records in log data associated with song plays i.e. records with page NextSong    
**Design decisions** : songplay_id column is a increasing number column
```
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```
 
### Dimension Tables
**users** : Users in the app  
**Design decisions** : 
```
user_id, first_name, last_name, gender, level  
```
**songs** : Songs in music database  
**Design decisions** : 
```
song_id, title, artist_id, year, duration  
```  
**artists** : Artists in music database  
**Design decisions** : 
```
artist_id, name, location, latitude, longitude  
```
**time** : Timestamps of records in songplays broken down into specific units  
**Design decisions** : 
```
start_time, hour, day, week, month, year, weekday
```

### Analytics Table
**user_listen** : Table to identify at which level(paid/free) did users listened most songs
**Design decisions** : 
```
first_name, paidUser_Count, freeUser_count
```

## Cleaned Outputs
Spark Dataframes are saved in parquet & csv formats in S3 Buckets by ```etl.py```. Below are the details,

**Files saved in Parquet Format**
1. **songs_table** : partitioned by year & artist, since songs_table contains artist_id, so instead of artist_name, artist_id is used.  
2. **time_table** : partitioned by year & month
3. **songplays_table** : partitioned by year & month
4. **artists_table** : Non-partitioned
5. **users_table** : Non-partitioned

**Files saved in CSV Format**
1. **log_clean.csv** : Cleaned log dataset
2. **song_clean.csv** : Cleaned song dataset
3. **user_listen.csv** : Analytics output

**Analytics**  

Below graph shows users who upgraded their services from FREE to PAID listened to more songs.  

![alt text](./images/chart1.png "Free/Paid Users")

## Issues

### **After boto3 installation in EMR Cluster AWS CLI stopped working**
After boto3, below command might not work

Copying files from current directory to S3
```
aws s3 sync . s3://sushanth-dend-datalake-programs
```

Copyings files from S3 to current EC2 directory
```
aws s3 sync s3://sushanth-dend-datalake-programs . 
```

You might need to do this. [check link](https://github.com/aws/aws-cli/issues/3542)  
```
pip install botocore==1.10.82
```

**Resolution** : changed approach to use AWS cli only before installing boto3. 

### **Not able to read S3 Bucket from Jupyter Notebooks**

```
Py4JJavaError: An error occurred while calling o1717.parquet.
: java.io.IOException: No FileSystem for scheme: s3
	at org.apache.hadoop.fs.FileSystem.getFileSystemClass(FileSystem.java:2660)
	at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2667)
	at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:94)
	at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2703)
	at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2685)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:373)
	at org.apache.hadoop.fs.Path.getFileSystem(Path.java:295)
	at org.apache.spark.sql.execution.datasources.DataSource.planForWritingFileFormat(DataSource.scala:424)
	at org.apache.spark.sql.execution.datasources.DataSource.planForWriting(DataSource.scala:524)
	at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:290)
	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:271)
	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:229)
	at org.apache.spark.sql.DataFrameWriter.parquet(DataFrameWriter.scala:566)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:238)
	at java.lang.Thread.run(Thread.java:748)

From
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
To
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
```

Understanding of S3 
* s3 Native FileSystem(single file can be upto only 5GB)
* s3n supports objects up to 5GB in size
* s3a supports objects up to 5TB and has higher performance

Approach Tried : 
Changed to 2.7.4, 2.7.7  
Still did not work  

### **Had issues in reading .JSON datasets from S3**
Took a long time to execute but 0 rows.  
![alt text](./images/emr-jupyter-notebook/song_df-s3nodata.png "SCP")

**Resolution** : Download all the files from the S3 Bucket outside of spark to local and analyzed it. Also another thought is dont assume spark will read all the subdirectories give path with asterix. 


### **Python programs written in V3 were run in V2 in EMR Cluster**
```
[hadoop@ip-172-31-33-17 ~]$ python -V
Python 2.7.16
```

This is how i found out
```
[hadoop@ip-172-31-33-17 ~]$ cat etl-emrB.txt
Downloading song_data
Traceback (most recent call last):
  File "/home/hadoop/./etl-emrB.py", line 394, in <module>
    main()
  File "/home/hadoop/./etl-emrB.py", line 367, in main
    download_files_from_s3(sampleDbBucket, 'song_data', 20000)
  File "/home/hadoop/./etl-emrB.py", line 63, in download_files_from_s3
    os.makedirs(folder, exist_ok=True)  # succeeds even if directory exists.
```

### **Multiple versions of Python installed on EMR Cluster**

Multiple versions of python were already installed in the machine due to that encountered below issue
```
[hadoop@ip-172-31-33-17 ~]$ python3 S3Download-emr.py
Traceback (most recent call last):
  File "S3Download-emr.py", line 10, in <module>
    import boto3
ModuleNotFoundError: No module named 'boto3'
```
**Resolution** : Changed the approach

### **Python program looking for file in HDFS  filesystem**

Spark looking for input file in 
```user/root/merged_song_data.json```

```
Error Message
Starting to process song data
Traceback (most recent call last):
  File "/home/hadoop/./etl-emrA.py", line 366, in <module>
    main()
  File "/home/hadoop/./etl-emrA.py", line 347, in main
    songs_table, artists_table = process_song_data(spark, input_data_song)
  File "/home/hadoop/./etl-emrA.py", line 92, in process_song_data
    schema=song_schema)
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 274, in json
  File "/usr/lib/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py", line 1257, in __call__
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 69, in deco
pyspark.sql.utils.AnalysisException: u'Path does not exist: hdfs://ip-172-31-33-17.us-west-2.compute.internal:8020/user/root/merged_song_data.json;'
19/11/02 08:19:57 INFO SparkContext: Invoking stop() from shutdown hook
19/11/02 08:19:57 INFO SparkUI: Stopped Spark web UI at http://ip-172-31-33-17.us-west-2.compute.internal:4040
19/11/02 08:19:57 INFO YarnClientSchedulerBackend: Interrupting monitor thread
19/11/02 08:19:57 INFO YarnClientSchedulerBackend: Shutting down all executors
19/11/02 08:19:57 INFO YarnSchedulerBackend$YarnDriverEndpoint: Asking each executor to shut down
19/11/02 08:19:57 INFO SchedulerExtensionServices: Stopping SchedulerExtensionServices
(serviceOption=None,
 services=List(),
 started=false)
19/11/02 08:19:57 INFO YarnClientSchedulerBackend: Stopped
19/11/02 08:19:57 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
19/11/02 08:19:57 INFO MemoryStore: MemoryStore cleared
19/11/02 08:19:57 INFO BlockManager: BlockManager stopped
19/11/02 08:19:57 INFO BlockManagerMaster: BlockManagerMaster stopped
19/11/02 08:19:57 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
```
**Resolution** : Copied the files to HDFS
```
hadoop fs -ls /user/root

hadoop fs -put ./merged_log_data.json /user/root/merged_log_data.json
hadoop fs -put ./merged_song_data.json /user/root/merged_song_data.json

hadoop fs -ls /user/root
```

Below commands can be used create new directories
```
hdfs dfs -mkdir /home
hdfs dfs -mkdir /home/hadoop
```

### **Program failed in EMR due to finding unicode character while displaying the data(df.show()).**
```
UnicodeEncodeError: 'ascii' codec can't encode character u'\xfc' in position 617: ordinal not in range(128)
```
Below is the statement it failed on ```artists_table.show(5)```

Below is the unicode character
```
df = spark.createDataFrame([[u'\xfc']])
df.show()
+---+
| _1|
+---+
|  ü|
+---+
```
**Resolution** : It shouldn't have failed, if spark was using Python 3. Other solution is to use encode(). I have been using this for just validation purposes, to resolve the issue, i have commented the statement.


## Learnings  

### **Udacity .JSON dataset**
Below dataset contains more than 14k files from S3 perspective, quota is accessing 20k objects, in couple of 

### **[EC2 connection timeout](https://stackoverflow.com/questions/5881461/ec2-network-error-connection-timeout)**

You simply need to add an ssh rule for inbound connections to you ec2 instance in the ec2 management console.

* Go to ec2 console
* Click Instances on Left
* Select your instance
* In the Description tab, locate Security Groups and click the available group link
* Click edit button on Inbound tab
* Click Add Rule and select SSH for type, Port Range 22, and Source Anywhere
* Connect with putty :)

### **inferschema**
Big Data Tech is all about Schema-on-read which is upoad read the data is checked against the specified schema.
```
df = spark.read.csv("s3a://udacity-labs/pagila/payment.csv", sep=";", inferSchema=True, header=True)
```
inferSchema = assume schema from data


### **[Reading a big file and writing limited number of lines](https://stackoverflow.com/questions/44277019/how-to-read-only-n-rows-of-large-csv-file-on-hdfs-using-spark-csv-package)**

Read the file as a text file, take as many lines as you want and save it to some temporary location. With the lines saved, you could use spark-csv to read the lines, including inferSchema option (that you may want to use given you are in exploration mode).
```
val numberOfLines = ...
spark.
read.
text("myfile.csv").
limit(numberOfLines).
write.
text(s"myfile-$numberOfLines.csv")
val justFewLines = spark.
read.
option("inferSchema", true). // <-- you are in exploration mode, aren't you?
csv(s"myfile-$numberOfLines.csv")
```

### **spark.read.json() doesn't read all the subdirectories**
Expected below to read all the files in the subdirectories but it did not.
```
path = "data/song-data/song_data"
song_df = spark.read.json(path, 
          schema=song_schema)
```

Below works,  
```path = "data/song-data/song_data/*/*/*/*.json"```

### **Timing Processes**
```
from time import time      

t0 = time()

<<process>>

uploadTime = time()-t0
print("=== {} Uploaded in: {} sec\n".format(table, round(uploadTime,2) ))
```

### **Knowing Cluster OS details**

```cat /etc/os-release```

```
[hadoop@ip-172-31-33-17 ~]$ cat /etc/os-release
NAME="Amazon Linux AMI"
VERSION="2018.03"
ID="amzn"
ID_LIKE="rhel fedora"
VERSION_ID="2018.03"
PRETTY_NAME="Amazon Linux AMI 2018.03"
ANSI_COLOR="0;33"
CPE_NAME="cpe:/o:amazon:linux:2018.03:ga"
HOME_URL="http://aws.amazon.com/amazon-linux-ami/"
```

### **Only python print() statements are redirected**
Writes only the print statement to the output redirected file
```
sudo /usr/bin/spark-submit --master yarn ./etl-emrA.py > etl-emrA.txt
```
Spark INFO lines are not written. 


### **Jupyter installations are messy in EMR**
* Not able to install PySpark & Other python packages

Below technique wasn't that effective
```
import sys
!{sys.executable} -m pip install numpy
```

* **Solution** : Change the kernel to PySpark and install packages
```
sc.list_packages()

sc.install_pypi_package("pandas==0.25.1") #Install pandas version 0.25.1  

sc.install_pypi_package("matplotlib", "https://pypi.org/simple") #Install matplotlib from given PyPI repository

sc.list_packages() #Recheck the list for validation

# And then import
import numpy as np
import matplotlib.pyplot as plt
```
### **Faster way to transfer files from Udacity workspace to EMR Cluster**

There are two methods scp is efficient, 
1. S3
        a. Download from Udacity workspace to local 
        b. Drag and drop to S3 Bucket
        c. run AWS CLI command in EMR cluster download the file

        Problem : When you install boto3 in EMR Cluster. AWS CLI command fails to run. Thats why you have to run AWS CLI before installing the boto3. 
2. SCP
        a. Download the file Udacity workspace to local
        b. When you created a EC2-KeyPair, you would have got a .pem file. Keep that ready
        c. Run the below SCP command to transfer files from local to EMR cluster -> hadoop user -> home directory  
```scp -i spark-cluster.pem etl-emrB.py hadoop@ec2-34-222-200-133.us-west-2.compute.amazonaws.com:~/.```  
   
### **Reading material on JobSteps ActionOnFailure**  
ActionOnFailure = TERMINATE_JOB_FLOW | TERMINATE_CLUSTER | CANCEL_AND_WAIT | CONTINUE
https://stackoverflow.com/questions/15281954/elastic-map-reduce-difference-between-cancel-and-wait-and-continue  
Say you have launched a cluster and added following 3 steps to it:  

Step1  
Step2  
Step3  

* Now, if Step1 has ActionOnFailure as CANCEL_AND_WAIT, then in the event on failure of Step1, it would cancel all the remaining steps and the cluster will get into a Waiting status. And I guess if you laucng your cluster with --stay-alive option then this is the default behaviour.  

* if Step1 has ActionOnFailure as CONTINUE, then in the event on failure of Step1, it would continue with the execution of Step2.  

* if Step1 has ActionOnFailure as TERMINATE_JOB_FLOW, then in the event on failure of Step1, it would shut down the cluster as you mentioned.  
<hr>

### Some linux commands
* ```pip freeze``` : to know python packages installed in the box

* ```type python``` : Similar ```which python``` command
```
[hadoop@ip-172-31-39-239 ~]$ type python
python is hashed (/usr/bin/python)
[hadoop@ip-172-31-39-239 ~]$ type python3
python3 is /usr/bin/python3
```

## Links
### Python Packages Installation
* [EMR Notebooks : Installing libraries in PySpark](https://aws.amazon.com/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/)  
* [Handling package installation issues in Jupyter](https://jakevdp.github.io/blog/2017/12/05/installing-python-packages-from-jupyter/)

* [Generating unique numbers in spark(monotonically increasing id)](https://stackoverflow.com/questions/48209667/using-monotonically-increasing-id-for-assigning-row-number-to-pyspark-datafram)
* [Sequential IDs in Spark](https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6)


