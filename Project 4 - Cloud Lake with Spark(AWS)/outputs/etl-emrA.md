Below is the execution output of etl-emrA.py 
It took 4664.07 seconds to complete and writing parquet files to S3 took majority of the time due to the number of objects 14k+

```
Starting to process song data
=== Read songs file Total Elapsed time is 3.73 sec

=== Process songs file Total Elapsed time is 5.64 sec

+------------------+--------------------+------------------+----+---------+
|           song_id|               title|         artist_id|year| duration|
+------------------+--------------------+------------------+----+---------+
|SOTKKVF12A67AD8466|I'll Know (2003 D...|ARHW1LO1187B9A8561|   0|154.64444|
|SOIYNBM12AB01825C3|           Jes' Fine|AR0PGDM1187B990E66|   0| 331.8069|
|SOAPYHO12A6701E3DF|Stone Groove (Alb...|ARJ47041187B98E500|2004|285.88364|
|SOAGVJV12AF729DACF|Brushing Of The W...|ARV15CM1187B990EEA|   0|290.19382|
|SOCQJBX12AF72AA6F2|The Owl and the P...|ARKCTSM11F4C83C839|   0|156.70811|
+------------------+--------------------+------------------+----+---------+
only showing top 5 rows

+------------------+--------------------+--------------------+--------+---------+
|         artist_id|                name|            location|latitude|longitude|
+------------------+--------------------+--------------------+--------+---------+
|ARLDX1T1187B991E69|           Joe Veras|Cotui, Dominican ...|19.05871|-70.15213|
|ARABYN61187B997D4E|            Flipsyde|               Texas|     0.0|      0.0|
|ARKXBAH1187FB3F8AD|     The Futureheads|          Sunderland|54.90012| -1.40848|
|ARW91B61187B989DFD|        Lewis Taylor|                    |     0.0|      0.0|
|AR740W81187FB3943B|Bernard Haitink_ ...|           Amsterdam|     0.0|      0.0|
+------------------+--------------------+--------------------+--------+---------+
only showing top 5 rows

Starting to process log data
=== Process log files : Read & Transformation Total Elapsed time is 4.29 sec

Creating users table
=== Creating users table Total Elapsed time is 0.08 sec

Creating user_listen table
=== Creating user_listen table Total Elapsed time is 0.12 sec

Creating time table
=== Creating time table Total Elapsed time is 0.28 sec

=== Process log files : Total Total Elapsed time is 4.77 sec

+--------------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+----------------+------+-------------+--------------------+------+--------------------+----------------------+
|        artist|     auth|firstName|gender|itemInSession|lastName|   length|level|            location|method|    page|     registration|sessionId|            song|status|           ts|           userAgent|userId|        ts_converted|registration_converted|
+--------------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+----------------+------+-------------+--------------------+------+--------------------+----------------------+
|Gustavo Cerati|Logged In|    Adler|     M|            1| Barrera|249.44281| free|New York-Newark-J...|   PUT|NextSong|1.540835983796E12|      248|  Uno Entre 1000|   200|1541470383796|"Mozilla/5.0 (Mac...|   100|2018-11-06 02:13:...|         1540835983796|
|   Limp Bizkit|Logged In|    Adler|     M|            2| Barrera|270.49751| free|New York-Newark-J...|   PUT|NextSong|1.540835983796E12|      248|Behind Blue Eyes|   200|1541470632796|"Mozilla/5.0 (Mac...|   100|2018-11-06 02:17:...|         1540835983796|
|Mikel Erentxun|Logged In|   Samuel|     M|            1|Gonzalez|178.83383| free|Houston-The Woodl...|   PUT|NextSong|1.540492941796E12|      252|    Frases Mudas|   200|1541474048796|"Mozilla/5.0 (Mac...|    61|2018-11-06 03:14:...|         1540492941796|
|   The Gerbils|Logged In|   Martin|     M|            0| Johnson| 27.01016| free|Minneapolis-St. P...|   PUT|NextSong|1.541081807796E12|      250|           (iii)|   200|1541480171796|"Mozilla/5.0 (Mac...|    55|2018-11-06 04:56:...|         1541081807796|
|           AFI|Logged In|   Martin|     M|            1| Johnson|190.45832| free|Minneapolis-St. P...|   PUT|NextSong|1.541081807796E12|      250| Girl's Not Grey|   200|1541480198796|"Mozilla/5.0 (Mac...|    55|2018-11-06 04:56:...|         1541081807796|
+--------------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+----------------+------+-------------+--------------------+------+--------------------+----------------------+
only showing top 5 rows

+-------+----------+---------+------+-----+
|user_id|first_name|last_name|gender|level|
+-------+----------+---------+------+-----+
|     26|      Ryan|    Smith|     M| free|
|      7|    Adelyn|   Jordan|     F| free|
|     71|    Ayleen|     Wise|     F| free|
|     81|    Sienna|    Colon|     F| free|
|     87|    Dustin|      Lee|     M| free|
+-------+----------+---------+------+-----+
only showing top 5 rows

+--------------------+---+-----+----+----+------+------+----+-------+
|          start_time|day|month|year|hour|minute|second|week|weekday|
+--------------------+---+-----+----+----+------+------+----+-------+
|2018-11-06 22:31:...|  6|   11|2018|  22|    31|    18|  45|      3|
|2018-11-14 11:45:...| 14|   11|2018|  11|    45|    58|  46|      4|
|2018-11-05 08:42:...|  5|   11|2018|   8|    42|    46|  45|      2|
|2018-11-05 17:23:...|  5|   11|2018|  17|    23|    52|  45|      2|
|2018-11-05 18:46:...|  5|   11|2018|  18|    46|    56|  45|      2|
+--------------------+---+-----+----+----+------+------+----+-------+
only showing top 5 rows

+----------+-------+-------+
|first_name|puCount|fuCount|
+----------+-------+-------+
|     Rylan|    221|     23|
|Jacqueline|    372|      9|
|  Mohammad|    266|     46|
|      Lily|    491|      4|
|   Kinsley|    198|      6|
+----------+-------+-------+
only showing top 5 rows

Processing songplays
=== Process songplays Total Elapsed time is 0.69 sec

Show spark sql table row counts
+---------+-----+
|   Tables| Rows|
+---------+-----+
|    users|  104|
|    songs|14896|
|  artists|10025|
|     time| 6813|
|songplays|  822|
+---------+-----+

Creating s3 bucket sushanth-dend-datalake-parquet-files2
-- Bucket Created
Starting to write parquet files
Output path ./output2
Starting to write CSV files
Starting upload to s3
Uploading artists.parquet files
=== artists.parquet Uploaded in: 58.13 sec

Uploading songplays.parquet files
=== songplays.parquet Uploaded in: 0.78 sec

Uploading songs.parquet files
=== songs.parquet Uploaded in: 4230.45 sec

Uploading time.parquet files
=== time.parquet Uploaded in: 59.22 sec

Uploading users.parquet files
=== users.parquet Uploaded in: 24.73 sec

Uploading log_clean.csv files
=== log_clean.csv Uploaded in: 1.17 sec

Uploading song_clean.csv files
=== song_clean.csv Uploaded in: 0.73 sec

Uploading user_listen.csv files
=== user_listen.csv Uploaded in: 2.66 sec

=== Write parquet files Total Elapsed time is 4583.02 sec

=== Main() Total Elapsed time is 4664.07 sec

Done!
```