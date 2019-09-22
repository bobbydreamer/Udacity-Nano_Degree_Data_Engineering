# Data Modeling with Cassandra  
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Project Overview
In this project, you will load CSV files into dataframes, assess and clean the data and denormalize the dataframe (or) Apache Cassandra tables to suit the queries.

## Goal
1. Get artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Get the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Get every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

## Project Step-by-Step
### Gather
1. Read multiples files in ./data folder is read and combined into single file (event_datafile_new.csv) and loaded into a dataframe df

### Assess
1. df : ts column in datafile can be converted to timestamp and used, if required.
2. df analyzed after having a look at

    **Quality**
    1. Remove duplicates from df
    2. Check of duplicates & remove after splitting df

    **Tidiness**
    1. Change ts column from float to timestamp to string.  
        a. When trying to load cassandra table, encountered error for this timestamp column. But it seemed to load successfully when dataframe timestamp column is of type(string). So converting this timestamp column to string.  

    2. Split df into 3 separate dataframes(appHist, songHist, userHist) to store rows as per queries  

### Clean

### Data Modeling (Panda dataframes)

Below dataframes are designed based on the query

**1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4**  
```
    appHist_df = df_clean[['sessionId', 'itemInSession', 'artist', 'song', 'length']]  
    appHist_df[(appHist_df.sessionId == 338) & (appHist_df.itemInSession == 4)]
```    

**2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182**  
```
    userHist_df = df_clean[['userId', 'sessionId', 'itemInSession', 'firstName', 'lastName', 'artist', 'song']]  
    userHist_df[(userHist_df.userId == 10) & (userHist_df.sessionId == 182)]
```    

**3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'**  
```
    songHist_df = df_clean[['song', 'userId', 'firstName', 'lastName', 'ts']]
    songHist_df[(songHist_df.song =='All Hands Against His Own')]
``` 

Included ts(timestamp) as user had listened to the same song at different times or sessions. Since this dataframe is song history, thought it should have timestamp as well.  

### Data Modeling (Apache Cassandra)
Below dataframes are designed based on the query  

**1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4**  

Design decisions,  
1. Expected output from query is (artist, song, length)  
2. Query condition ( sessionId & itemInSession )
3. Composite Primary Keys are chosen based on the query conditions. sessionId as the leading column and itemInSession is added as an additional column to support the WHERE condition and to avoid using 'ALLOW FILTERING' in query.
```
CREATE TABLE IF NOT EXISTS app_history(
sessionId INT,
itemInSession INT,
artist VARCHAR,
song VARCHAR,
length DECIMAL,
PRIMARY KEY(sessionId, itemInSession)
);
```

Below error can be encountered when create table with primary key only (sessionId)
```
Error from server: code=2200 [Invalid query] message="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
```

**2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182**  

Design decisions,  
1. Expected output from query is (artist, song, user)  
2. Query condition ( userId & sessionId )
3. Composite Primary Keys are chosen based on the query conditions. userId as the leading column, sessionId is added as an additional column to support the WHERE condition and itemInSession is added to support clustering. 
4. firstName & lastName is not added to the primary key for two reasons(a. they are not used in WHERE condition b. Data looked already clustered without them)

```
CREATE TABLE IF NOT EXISTS user_history (
userId INT,
sessionId INT,
itemInSession INT,
firstName VARCHAR,
lastName VARCHAR,
artist VARCHAR,
song VARCHAR,
PRIMARY KEY(userId, sessionId, itemInSession)
);
```
**3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'** 

Design decisions,  
1. Expected output from query is name(firstName, lastName)  
2. Query condition ( song )
3. Composite Primary Keys are chosen based on the query conditions. song as the leading column as its used in the query. Remaining columns were added for uniqueness due to the reason same user has listened to the same song multiple times. So timestamp is added to maintain that uniqueness.  

```
CREATE TABLE IF NOT EXISTS song_history (
song VARCHAR,
userId INT,
firstName VARCHAR,
lastName VARCHAR,
ts timestamp,
PRIMARY KEY(song, userId, ts)
);
```
### ETL Pipeline
1. All the CSV files are combined to event_datafile_new.csv
2. event_datafile_new.csv is read to primary dataframe(df) 
3. df is cleaned and split into below 3 dataframes  
a. appHist_df  
b. userHist_df  
c. songHist_df  
4. Each dataframe is loaded into separate Apache Cassandra table