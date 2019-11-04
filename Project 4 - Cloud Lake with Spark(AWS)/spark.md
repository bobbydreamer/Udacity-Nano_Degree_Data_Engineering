# Spark Fundamentals  

> Notes are not in order

## Big Data

**CPU** - Brain of the computer. Typical CPU 2.5 Gifahertz CPU processes 2.5 billion operations per second. For each operation, CPU processes 8 bytes of data. So, in a second this CPU can process 20 billion bytes per second. 2.5 billion operations per second x 8 bytes per operation = 20 billion bytes per second  

CPU - 200x faster than memory

Twitter generates about 6,000 tweets per second, and each tweet contains 200 bytes. So in one day, Twitter generates data on the order of:  
(6000 tweets / second) x (86400 seconds / day) x (200 bytes / tweet) = 104 billion bytes / day  
Knowing that tweets create approximately 104 billion bytes of data per day, how long would it take the 2.5 GigaHertz CPU to analyze a full day of tweets?
104 billion bytes * (1 second / 20 billion bytes) = 5.2 seconds  

**RAM** - data gets temporarily stored in memory before getting sent to the CPU. Its a ephemeral storage.  

**Storage** (SSD or Magnetic Disk) - Storage is used for keeping data over long periods of time.  

**Memory** - 15x faster than ssd

**Storage** - 20x faster than network

Distributed systems or cluster is a bunch of connected machines and each machine can be called a node.  

If a dataset is larger than the size of your RAM, you might still be able to analyze the data on a single computer. By default, the Python pandas library will read in an entire dataset from disk into memory. If the dataset is larger than your computer's memory, the program won't work.

However, the Python pandas library can read in a file in smaller chunks. Thus, if you were going to calculate summary statistics about the dataset such as a sum or count, you could read in a part of the dataset at a time and accumulate the sum or count

Read everything to memory  
```table = pd.read_csv('tmp.sv', sep='|')```

Read in Chunks(rows)  
```
reader = pd.read_csv('tmp.sv', sep='|', chunksize=4)
for chunk in reader:
print(chunk)
```
(or)
```
reader = pd.read_csv('tmp.sv', sep='|', iterator=True)
reader.get_chunk(5)
```

### **Distributed Vs. Parallel computing**
* Distributed Computing each CPU has its own memory and each computer/node is connected to other machines via network
* Parallel Computing implies multiple CPUs share the same memory

### **Hadoop Vocabulary**
Here is a list of some terms associated with Hadoop. You'll learn more about these terms and how they relate to Spark in the rest of the lesson.

Hadoop - an ecosystem of tools for big data storage and data analysis. Hadoop is an older system than Spark but is still used by many companies. The major difference between Spark and Hadoop is how they use memory. Hadoop writes intermediate results to disk whereas Spark tries to keep data in memory whenever possible. This makes Spark faster for many use cases.

Hadoop MapReduce - a system for processing and analyzing large data sets in parallel.

Hadoop YARN - a resource manager that schedules jobs across a cluster. The manager keeps track of what computer resources are available and then assigns those resources to specific tasks.

Hadoop Distributed File System (HDFS) - a big data storage system that splits data into chunks and stores the chunks across a cluster of computers.

As Hadoop matured, other tools were developed to make Hadoop easier to work with. These tools included:

Apache Pig - a SQL-like language that runs on top of Hadoop MapReduce  
Apache Hive - another SQL-like interface that runs on top of Hadoop MapReduce

How is Spark related to Hadoop?  
Spark, which is the main focus of this course, is another big data framework. Spark contains libraries for data analysis, machine learning, graph analysis, and streaming live data. Spark is generally faster than Hadoop. This is because Hadoop writes intermediate results to disk whereas Spark tries to keep intermediate results in memory whenever possible.  

The Hadoop ecosystem includes a distributed file storage system called HDFS (Hadoop Distributed File System). Spark, on the other hand, does not include a file storage system. You can use Spark on top of HDFS but you do not have to. Spark can read in data from other sources as well such as Amazon S3.  

### **Hadoop MapReduce** ( Power of Spark - 14.MapReduce )

The technique works by first dividing up a large dataset and distributing the data across a cluster. In the map step, each data is analyzed and converted into a (key, value) pair. Then these key-value pairs are shuffled across the cluster so that all keys are on the same machine. In the reduce step, the values with the same keys are combined together.

Map -> Shuffle -> Reduce

## Spark

* Master-worker hiearchy
* Local Mode : Single Machine : No distributed computing : Learn Syntax & Prototyping
* Distributed : Requires Cluster Manager. Three types of cluster manager  
  a. Standalone  
  b. YARN : Sharing cluster with team  
  c. MESOS : Sharing cluster with team  

* Usually you are interacting with Spark driver not with the cluster directly
* Spark Written in Scala
* usually programming style is procedural for Spark one needs to learn procedural programming.
* In distributed systems, functions shouldn't have side affects from variables out side their scope.

* Spark before doing any processing, it builds step-by-step functions & data it will need. This is called directed Acyclical Graph(DAG). it waits until last moment to get the data. This multi-step combos are called stages.

* HDFS splits files in 64MB or 128MB block and replicates the block across the cluster

* **SparkContext** : Connects cluster with application
```
from pyspark import SparkContext, SparkConf
```

* Setting local mode  
```configure = SparkConf().setAppname("name").setMaster("local")```

* Setting up Master node Ip Address  
```
configure = SparkConf().setAppname("name").setMaster("Ip Address")
sc = SparkContext(conf = configure)
```

To read data you need SparkSession  
Sample spark - Reading & Writing Spark Dataframes  
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Our first spark sql example").getOrCreate()
spark.sparkContext.getConf().getAll()

path = "hdfs://ec2-34-218.../sparkify_log_small.json"
user_log = spark.read.json(path)
user_log.printSchema()
user_log.describe()
user_log.show(n=1)
user_log.take(5)
out_path = "hdfs://....sparkify_log_file.csv"
user_log.write.save(out_path, format = "csv", header = True)
user_log_2 = spark.read.csv(out_path, header=True)
user_log_2.take(2)
```

Imperative Programming : Spark DataFrames & Python
Declarative Programming : SQL

Stats meaningful only when used on a numeric column
```
user_log.show()
user_log.describe("artist").show()
user_log.count()

user_log.select("page").dropDuplicates().sort("page").show()
user_log.select(["userId", "firstname", "page", "song"]).where(user_log.userId == "1045").collect()

songs_in_hour = user_log.filter(user_log.page == "NextSong").groupby(user_log.hour).count().orderBy(user_log.hour.cast("float"))
songs_in_hour.show()

song_in_hour_pd = songs_in_hour.toPandas()
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")

flag_downgrade_event = udf(lambda x: 1 if x =="submit Downgrade" else 0, IntegerType()))
user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))

frompyspark.sql import Window
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded").over(windowval))
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log_userId == "1138").collect()
```

Spark SQL  
```
user_log.createOrReplaceTempView("user_log_table")
spark.sql("select * from user_log_table limit 2)
spark.sql('''
select * from user_log_table limit 2
''').show()
```

UDFs have to be registered  
```
spark.udf.register("get_hour", lambda x:int(datetime.datetime.fromtimestamp(x / 1000.0).hour))
spark.sql('''
select *, get_hour(ts) as hour from user_log_table limit 2
''').collect()

songs_in_hour = spark.sql('''
select *, get_hour(ts) as hour from user_log_table limit 2
''')
song_in_hour_pd = songs_in_hour.toPandas()
print(song_in_hour_pd)
```

* RDD(Resilient Distributed Dataset)  

distributed_song_log.map(convert_song_to_lowercase)  
You'll notice that this code cell ran quite quickly. This is because of lazy evaluation. Spark does not actually execute the map step unless it needs to.

"RDD" in the output refers to resilient distributed dataset. RDDs are exactly what they say they are: fault-tolerant datasets distributed across a cluster. This is how Spark stores data.

To get Spark to actually run the map step, you need to use an "action". One available action is the collect method. The collect() method takes the results from all of the clusters and "collects" them into a single list on the master node.
```
distributed_song_log.map(convert_song_to_lowercase).collect()

sparkify_log_data = "s3n://sparkify/sparkify_log_small_2.json"

df = spark.read.json(sparkify_log_data)
df.persist()

df.head(5)
```
df.persist() = persists the data on spark cluster. Store data in HDFS file system  

* Sparks Optimizer is called Catalyst

### Schema-on-read  
It is possible to make data analysis without inserting into a predefined schema. One can load a CSV fiel and make a query without creating a table, inserting the data in the table. Similarly one can process unstructured text. This approach is known as "Schema-On-Read"

Data warehouse by design follows a very-well-architected path to make a clean, consistent and performant model that business users can easily use to gain insights and make decisions. 

Data lake is a new form of a data warehouse that evolved to cope with : 
* Variety of data formats and structuring
* Agile and adh-c nature of data exploration activities needed by new roles like the data scientist. 
* Supports wide spectrum of analytics like machine learning, graph analytics. 

Schema on read - schema inference
```
dfPayment = spark.read.csv("data/pagila/payment.csv", 
                           inferSchema=True,
                           header=True,
                           sep=";")
dfPayment.printSchema()
```
Infer schema will automatically guess the data types for each field. If we set this option to TRUE, the API will read some sample records from the file to infer the schema. If we want to set this value to false, we must specify a schema explicitly.

```
paymentSchema = StructType([
	StructField("payment_id", IntegerType()),
        StructField("payment_date", DateType())
])

dfPayment = spark.read.csv("data/pagila/payment.csv", 
                           schema=paymentSchema,
                           sep=";",
                           mode="DROPMALFORMED")
dfPayment.printSchema()
dfPayment.show(5)
```

SPARK SQL
```
from pyspark.sql.functions import desc

dfPayment.groupBy("customer_id")\
         .sum("amount")\
         .orderBy(desc("sum(amount)"))\
         .show(5)

dfPayment.createOrReplaceTempView("payment")
spark.sql("""
select customer_id, sum(amount) as revenue
from payment
group by customer_id
order by revenue desc 
""").show(5)
```

### Data formats
1. Text : JSON, CSV, Text
2. Binary : AVro(saves space), Parquet(columnar)
3. Compressed : gzip, snappy

### Spark Submit
```
# Location of the spark-submit installation
which spark-submit

# Syntax
/usr/bin/spark-submit --option(location of master) resource_negotiator

# spark-submit example
/usr/bin/spark-submit --master yarn ./lower_songs.py
````

### Some HDFS Commands

Checked hadoop files
```
hadoop fs -ls
```

Created folder in hadoop filesystem
```
hdfs dfs -mkdir /home
hdfs dfs -mkdir /home/hadoop
```

Copied file to that location
```
hadoop fs -put ./sparkify_log_small.json /home/hadoop/sparkify_log_small.json

hadoop fs -ls /home/hadoop/
```

Syntax to get files from HDFS  
```hadoop fs -get /<hdfs path> /<local machime path>```  

* [Top 10 hadoop shell commands](https://dzone.com/articles/top-10-hadoop-shell-commands)
* [Using hdfs command line to manage files and directories on Hadoop](http://fibrevillage.com/storage/630-using-hdfs-command-line-to-manage-files-and-directories-on-hadoop)


### Lesson 3: Debugging and Optimization : 6.Submitting Spark Scripts.

I am trying as show in the video between time 00:00 

I have created a AWS EMR Cluster and uploaded,
sparkify_log_small.json

And created a EMR Jupyter Notebook with below code thinking it would read from user(hadoop) home directory.
```
sparkify_log_data = "sparkify_log_small.json"
df = spark.read.json(sparkify_log_data)
df.persist()
df.head(5)
```

But when submit the code, i get the below error. 
```
'Path does not exist: hdfs://ip-172-31-50-58.us-west-2.compute.internal:8020/user/livy/sparkify_log_small.json;'
Traceback (most recent call last):
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 274, in json
    return self._df(self._jreader.json(self._spark._sc._jvm.PythonUtils.toSeq(path)))
  File "/usr/lib/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py", line 1257, in __call__
    answer, self.gateway_client, self.target_id, self.name)
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 69, in deco
    raise AnalysisException(s.split(': ', 1)[1], stackTrace)
pyspark.sql.utils.AnalysisException: 'Path does not exist: hdfs://ip-172-31-50-58.us-west-2.compute.internal:8020/user/livy/sparkify_log_small.json;'
```

From googling got to know that YARN default user is livy. How can i change the user in the jupyter notebook from livy to hadoop (or) point to the right directory. 

I have tried creating a folder like below and copying file from /home/hadoop/sparkify_log_small.json to /home/livy/sparkify_log_small.json

but did not work.

**Solution** : Copy the file to HDFS file system


### Simple spark program 
```
from pyspark.sql import SparkSession
#import pyspark
#sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")

def convert_song_to_lowercase(song):
    return song.lower()

if __name__ == "__main__":
"""
	example program to show how to submit applications
"""

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "Despacito",
        "All the stars"
]

#distributed_song_log = sc.parallelize(log_of_songs)
distributed_song_log = spark.sparkContext.parallelize(log_of_songs)

print(distributed_song_log.map(convert_song_to_lowercase).collect())
print(distributed_song_log.map(lambda song: song.lower()).collect()

spark.stop()
```


### PRECEDING AND FOLLOWING  
https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html  
https://alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/?source=post_page-----907f274850e4----------------------  
https://knockdata.github.io/spark-window-function/


### Creating EMR cluster using boto3  
https://www.edureka.co/community/39304/how-to-create-emr-cluster-using-python-boto3  
https://stackoverflow.com/questions/26314316/how-to-launch-and-configure-an-emr-cluster-using-boto  
https://docs.aws.amazon.com/code-samples/latest/catalog/python-emr-emrfs-boto-step.py.html  
https://becominghuman.ai/real-world-python-workloads-on-spark-emr-clusters-3c6bda1a1350  
https://towardsdatascience.com/getting-started-with-pyspark-on-amazon-emr-c85154b6b921  
https://www.oreilly.com/learning/how-do-i-connect-to-my-amazon-elastic-mapreduce-emr-cluster-with-ssh  

### AWS EMR(Elastic MapReduce)
* EC2 -> Keys Pairs in Network & Security
* Create new Keypair spark-cluster.pem
* half is pem other half in EC2 machines

Key pair name : spark-cluster

EC2 -> Create Keypair
Instally Putty & PuTTY Key Generator
1. Parameters(SSH-1(RSA))
2. Number of bits in a generated key : 2048
3. Load the spark-cluster.pem
4. Save private key as spark-cluster it will be saved with extension as .ppk

### EMR
* emr-5.20.0 -> spark 2.4.0
* m5.xlarge => m=multipurpose family, 5th generation of hardware comes with SSD Storage
* 0.048 per hour = 5cents perhour
* Number of instances : 4 (1 Mster and 3 core nodes)
* m5.xlarge = EC2($0.192 per hour) & EMR(0.048 per hour)

After starting the mer notebook, Change the kernel to PySpark

### Lesson 3 : Debugging & Optimization : Reading and Writing Data to HDFS
View All -> HDFS -> Utilities -> Browse the file system

1. SSH into EMR Cluster
2. Copy file to EMR Cluster ( scp sparkify_log_small.json spark-emr:~/ )
3. Create new folder in HDFS ( hdfs dfs -mkdir /user/sparkify_data )
4. List of "hdfs" commands
5. hdfs dfs -copyFromLocal sparkify_log_small.json /user/sparkify_data/

* Driver node coordinates WorkNodes. print() statements in program might get printed on the worker nodes

**How to use Accumulators**  
```
incorrect_records = SparkContext.accumulator(0,0)
incorret_records.value

def add_incorrect_record():
global incorret_records
incorrect_records +=1

from pyspark.sql.functions import udf
correct_ts = udf(lambda x: 1 if x.isdigit() else add_incorrect_record())

logs3 = logs3.where(logs3["_corrupt_record"].isNull()).withColumn("ts_digit", correct_ts(logs3.ts))
incorrect_records.value

logs3.collect()
incorrect_records.value
```

* Spark uses lazy evaluation and if we didn't specify any action., there will not be any result. Simple action is collect()


Spark internals
* Shuffling
* DAG(broken into stages and then into tasks)
* Stages

WebUI shows current configuration of the settings

Some protocols
* port 22 for SSH
* port 80 for HTML
* spark master uses 7077 to com with worker nodes
* jupyter notes 8888
* 4040 shows status active jobs

Spark Web UI configured ports 4040/8080 after master node ip address
Jobs for each action, a job is run (eg:-.head()). Jobs broken down to stages. Clicking Job links, you can see stages and see DAG Visualization. 
Clicking on stages link.  
* Stages
* Storage contains cached RDDs
* Environment configuration parameters
* Executors info about executors(resources, how many tasks they have run successfully)
* SQL


Review of the Log Data
```
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

spark=SparkSession.builder.config('spark.ui.port', 3000).getOrCreate()
#Spark uses standard log4j which can be set in configuration file or in spark context
#Only see error messages in log files
spark.sparkContext.setLogLevel("ERROR")
```

Spariky Business
1. Daily lisenting time for average user per day.
2. Monthly Active users(unique users atleast 1 song in a month)
3. Daily Active users in Past Month( Active & Total users)
4. Total Paid and Unpaid Users
5. Total Ads Served in past month

### Cohort Analysis
Behaviour of past user and new user(monthly analysis)  
%upgrades  
%cancels  

Skewed data
Pareto 80-20 rule(know users or intituion of users)

Change workload division
Partition the data

As the data increases, processing time increases.

Suggested "Other issues and How to Address Them"
In this lesson, we walked through various examples of issues you can debug based on error messages, loglines and stack traces.

We have also touched on another very common issue with Spark jobs that can be harder to address: everything working fine but just taking a very long time. So what do you do when your Spark job is (too) slow?

Insufficient resources  
Often while there are some possible ways of improvement, processing large data sets just takes a lot longer time than smaller ones even without any big problem in the code or job tuning. Using more resources, either by increasing the number of executors or using more powerful machines, might just not be possible. When you have a slow job it’s useful to understand

how much data you’re actually processing (compressed file formats can be tricky to interpret),
if you can decrease the amount of data to be processed by filtering or aggregating to lower cardinality,
and if resource utilization is reasonable.
There are many cases where different stages of a Spark job differ greatly in their resource needs: loading data is typically I/O heavy, some stages might require a lot of memory, others might need a lot of CPU. Understanding these differences might help to optimize the overall performance. Use the Spark UI and logs to collect information on these metrics.

If you run into out of memory errors you might consider increasing the number of partitions. If the memory errors occur over time you can look into why the size of certain objects is increasing too much during the run and if the size can be contained. Also, look for ways of freeing up resources if garbage collection metrics are high.

Certain algorithms (especially ML ones) use the driver to store data the workers share and update during the run. If you see memory issues on the driver check if the algorithm you’re using is pushing too much data there.

Data skew  
If you drill down on the Spark UI to the task level you can see if certain partitions process significantly more data than others and if they are lagging behind. Such symptoms usually indicate a skewed data set. Consider implementing the techniques mentioned in this lesson:

add an intermediate data processing step with an alternative key
adjust the spark.sql.shuffle.partitions parameter if necessary
The problem with data skew is that it’s very specific to a data set. You might know ahead of time that certain customers or accounts are expected to generate a lot more activity but the solution for dealing with the skew might strongly depend on how the data looks like. If you need to implement a more general solution (for example for an automated pipeline) it’s recommended to take a more conservative approach (so assume that your data will be skewed) and then monitor how bad the skew really is.

Inefficient queries  
Once your Spark application works it’s worth spending some time to analyze the query it runs. You can use the Spark UI to check the DAG and the jobs and stages it’s built of.

Spark’s query optimizer is called Catalyst. While Catalyst is a powerful tool to turn Python code to an optimized query plan that can run on the JVM it has some limitations when optimizing your code. It will for example push filters in a particular stage as early as possible in the plan but won’t move a filter across stages. It’s your job to make sure that if early filtering is possible without compromising the business logic than you perform this filtering where it’s more appropriate.

It also can’t decide for you how much data you’re shuffling across the cluster. Remember from the first lesson how expensive sending data through the network is. As much as possible try to avoid shuffling unnecessary data. In practice, this means that you need to perform joins and grouped aggregations as late as possible.

When it comes to joins there is more than one strategy to choose from. If one of your data frames are small consider using broadcast hash join instead of a hash join.

#### extract columns to create artists table  
```
artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']) \
.withColumnRenamed('artist_name','artist') \
.withColumnRenamed('artist_location','location') \
.withColumnRenamed('artist_longitude','longitude') \
.withColumnRenamed('artist_latitude','latitude').distinct()
```  

