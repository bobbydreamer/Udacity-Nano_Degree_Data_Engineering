# Cloud Data Warehouses (AWS)

> ## Udacity Data Engineer Nano Degree Project 3

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Overview
Build an ETL pipeline for a database hosted on Redshift. Load the table with data from S3 and perform some query analysis.

## Technical Overview
Project is implemented in AWS Cloud and uses following products, 
1. S3 Storage Buckets
2. Redshift

## Goal
1. Review/Analyze datasets from S3 in panda dataframes  
2. Clean datasets  
3. Create new clean datasets for loading redshift staging tables  
4. Launch a redshift cluster and create an IAM role that has read access to S3  
5. Create redshift tables (staging, fact & dimension)  
6. Load staging tables using COPY command  
7. Load fact & dimension tables  
    a. INSERT via SELECT  
    b. Unload from staging table and use COPY command to load fact & dimension  
8. Prepare query for data analysis  

## Project Step-by-Step
### Gather
### Assess  
    #### Quality
    #### Tidiness  
### Clean

## Data Modeling (Panda dataframes)  
## Data Modeling (Redshift)  
    **Table** :
    **<<Tablename>> : Design decisions**,  

**factSongPlays**  
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**dimUsers**  
user_id, first_name, last_name, gender, level

**dimSongs**  
song_id, title, artist_id, year, duration

**dimArtists**  
artist_id, name, location, lattitude, longitude

**dimTime**  
start_time, hour, day, week, month, year, weekday


## ETL Setup
* Setup Redshift Cluster
* Create tables
* Populate the dimension table
* Populate the fact table
* Run Analytical Queries
* Drop tables
* Delete Redshift Cluster


**Create Table Schemas**  
1. Design schemas for your fact and dimension tables
Write a SQL CREATE statement for each of these tables in sql_queries.py  
2. Complete the logic in create_tables.py to connect to the database and create these tables  
3. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.  
4. Launch a redshift cluster and create an IAM role that has read access to S3.  
5. Add redshift database and IAM role info to dwh.cfg.  
6. Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.  

**ETL Pipeline**  
1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.  
2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.  
3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.  
4. Delete your redshift cluster when finished.  

## SongPlay Data Analysis Queries


## References
[1. Warehousing Concepts & Terminologies - Reference Notes](Cloud_Data_Warehouses_Reference_Notes.md)  
[2. AWS & Redshift - Reference Notes](AWS_and_Redshift.md)

### Video References
1. ETL Notes from Introduction to Data Warehouses : ETL Demo:Step 3 : Data Analysis

2. ETL Notes from Introduction to Data Warehouses : ETL Demo:Step 4 : Create facts & dimensions table

3. ETL Notes from Introduction to Data Warehouses : ETL Demo:Step 5 : Inserts to facts & dimensions table

4. ETL Notes from Introduction to Data Warehouses : ETL Demo:Step 6 : Fact & Dimension Analysis

### StackOverFlow 
How can I safely create a nested directory?
https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory

Download a folder from S3 using Boto3
https://stackoverflow.com/questions/49772151/download-a-folder-from-s3-using-boto3

This technique is used for reading log_data
Reading the JSON File with multiple objects in Python
https://stackoverflow.com/questions/40712178/reading-the-json-file-with-multiple-objects-in-python


"Project 2a - Data Modeling with Postgres" deals with reading *.json files. 

### Learnings
1. **CAUTION : Listing all objects from udacity-dend bucket in S3**  
    ```
    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    sampleDbBucket =  s3.Bucket("udacity-dend")

    for my_bucket_object in sampleDbBucket.objects.all():
        print(my_bucket_object)
    ```
2. **Installing Python Packages from a Jupyter Notebook**
https://jakevdp.github.io/blog/2017/12/05/installing-python-packages-from-jupyter/

If you try running below command in iPython  

```
pip install geopy

The following command must be run outside of the IPython shell:

    $ pip install geopy

The Python package manager (pip) can only be used from outside of IPython.
Please reissue the `pip` command in a separate terminal or command prompt.

See the Python documentation for more information on how to install packages:

    https://docs.python.org/3/installing/
```   


Do this to install Python package in Jupyter Notebooks
```
import sys
!{sys.executable} -m pip install geopy
```

3. **Map Plotting**
https://geopandas.readthedocs.io/en/latest/gallery/create_geopandas_from_pandas.html  


4. **Use Dictionary for JSON**  
Dictionary .get() can be used to retreive data from json object. If a key is not found, it will return null. Below some address{}, doesn't have city key.
```
loc_details['city'] = geoData['address'].get('city')
loc_details['state'] = geoData['address'].get('state')
loc_details['country'] = geoData['address'].get('country')
```
Data
```
{'place_id': 497005, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright', 'osm_type': 'node', 'osm_id': 158851303, 'lat': '40.0884475', 'lon': '-74.3995939', 'display_name': 'Woodair Estates, Ocean County, New Jersey, USA', 'address': {'hamlet': 'Woodair Estates', 'county': 'Ocean County', 'state': 'New Jersey', 'country': 'USA', 'country_code': 'us'}, 'boundingbox': ['40.0484475', '40.1284475', '-74.4395939', '-74.3595939']}
```

5. geopy Timeout issue
https://gis.stackexchange.com/questions/173569/avoid-time-out-error-nominatim-geopy-open-street-maps

6. Check if a value in a column is null or not
https://stackoverflow.com/questions/41287171/iterate-through-dataframe-and-select-null-values

--
df['ts'] = pd.to_datetime(df['ts'], unit='ms')

Quality
1. log_df : Only page='NextSong' is required
2. log_df : Delete rows which has userId as null/blank
3. song_df: Around 6693 rows doesn't have location & lats & long.
4. song_df: Around 4762 rows have Year has 0
5. song_df: Format of location is not consistent. London, England/Texas/California - LA
6. song_df: Around 2926 rows has just the location name doesn't have latitude, longitude
7. song_df: artist_location has None & Blank & Integer values(iloc[103])
8. log_df : When auth='Logged Out' it doesn't have userId. But when 'Logged In', userId is captured. Due to this quality issue, you cannot say exactly how long a user id logged in. You can just make a guess by listing the longs.

Tidiness
1. log_df : convert ts from integer to timestamp
2. song_df: column num_songs can be removed as it has only one value ( 1 )

1. Geo Graph on artist location
2. User who has listened to most songs. 
3. User who has spent longest duration listening to songs. 
4. Popular songs
5. How many free & paid users
6. How many users from free became paid
