# Cloud Data Warehouses( AWS )

> ## Udacity Data Engineer Nano Degree Project 4

## Project Overview
## Technical Overview
## Goal

* **Primary goal** : 

* **Test** : 

## Files in the folder

**Python Programs**

**Juputer Notebooks**

**Other files**

## How to run
### Running Local  
**Program** : etl-emr-hdfs.py  
Input
```
s3://udacity-dend/song_data
s3://udacity-dend/log_data
```

Output : ``` s3://sushanth-dend-datalake-parquet-files3 ```

Below are the steps,  
* a
* 
* 

### Running in AWS
**Program** : etl-emr-hdfs.py  
Input
```
s3://udacity-dend/song_data
s3://udacity-dend/log_data
```

Output : ``` s3://sushanth-dend-datalake-parquet-files3 ```

Below are the steps,  
* a
* 
* 

## Project Progression Step-by-Step
### **Gather**
### **Assess**  

**Quality**

**Tidiness**  

### **Clean**

## Data Modeling (Panda dataframes)  
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

## Learnings
### **Udacity .JSON dataset**

## Links
### Python Packages Installation
* [EMR Notebooks : Installing libraries in PySpark](https://aws.amazon.com/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/)  

* 
* 