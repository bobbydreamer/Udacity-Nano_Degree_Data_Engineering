from pyspark.sql import SparkSession
#import pyspark
#sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")

def convert_song_to_lowercase(song):
    return song.lower()

if __name__ == "__main__":
	'''
		example program to show how to submit applications
	'''

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
	print(distributed_song_log.map(lambda song: song.lower()).collect())

	spark.stop()