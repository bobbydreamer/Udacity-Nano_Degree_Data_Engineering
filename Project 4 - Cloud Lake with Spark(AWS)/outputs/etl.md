#
```
[hadoop@ip-172-31-44-57 ~]$ ls -l
total 7912
-rw-rw-r-- 1 hadoop hadoop   17590 Nov  3 14:47 etl-emr-hdfs.py
-rw-rw-r-- 1 hadoop hadoop    6872 Nov  3 15:00 etl-emr-hdfs.txt
-rw-rw-r-- 1 hadoop hadoop   16232 Nov  3 15:08 etl.py
-rw-rw-r-- 1 hadoop hadoop    1182 Nov  3 15:28 etl.txt
-rwxr-xr-x 1 hadoop hadoop     227 Nov  3 14:47 install-libr.sh
-rw-rw-r-- 1 hadoop hadoop 4015187 Nov  3 14:47 merged_log_data.json
-rw-rw-r-- 1 hadoop hadoop 4028468 Nov  3 14:47 merged_song_data.json
[hadoop@ip-172-31-44-57 ~]$ cat etl.txt
Processing Song data
Starting to process song data
=== Read songs file Total Elapsed time is 69.11 sec

=== Process songs file Total Elapsed time is 69.62 sec

Processing Log data
Starting to process log data
=== Process log files : Read & Transformation Total Elapsed time is 7.66 sec

Creating users table
=== Creating users table Total Elapsed time is 0.02 sec

Creating user_listen table
=== Creating user_listen table Total Elapsed time is 0.05 sec

Creating time table
=== Creating time table Total Elapsed time is 0.1 sec

=== Process log files : Total Total Elapsed time is 7.84 sec

Processing songplays
Processing songplays
=== Process songplays Total Elapsed time is 0.24 sec

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

Creating s3 bucket sushanth-dend-datalake-parquet-files4
-- Bucket Created
Starting to write parquet files
Output path s3://sushanth-dend-datalake-parquet-files4
Starting to write CSV files
=== Write parquet files Total Elapsed time is 360.31 sec

=== Main() Total Elapsed time is 616.37 sec

Done!
[hadoop@ip-172-31-44-57 ~]$
```