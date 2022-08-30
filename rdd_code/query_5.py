import time
import csv
from pyspark.sql import SparkSession

start = time.time()

spark = SparkSession.builder.appName("query_5").getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile("hdfs://master:9000/files/charts.csv")
artists = sc.textFile("hdfs://master:9000/files/artists.csv")
mapping = sc.textFile("hdfs://master:9000/files/chart_artist_mapping.csv")

artists = artists.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0])
mapping = mapping.map(lambda s: tuple(s.split(",")))

#join to obtaing mapping of id_song and artist
mapping = mapping.map(lambda s: (s[1], s[0])).join(artists).map(lambda s: (s[1][0], s[1][1]))

rdd = rdd.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0]).\
		filter(lambda s: s[5]=="top200").map(lambda s: (s[0], (s[3][:4], s[7]))).join(mapping).\
			map(lambda s: ((s[1][0][0], s[1][1]), int(s[1][0][1])/69)).reduceByKey(lambda a, b: a+b).\
				map(lambda s: (s[0][0], (s[0][1], s[1]))).reduceByKey(lambda a, b: a if a[1]>b[1] else b).\
					map(lambda s: (s[0], s[1][0], s[1][1])).sortBy(lambda s: s[0]).map(lambda s: ','.join(str(d) for d in s))

rdd.coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/rdd_results/result_5")

finish = time.time()

process_time = finish-start

print("Query 5 took:", process_time, "seconds")
