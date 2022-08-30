import time
import csv
from pyspark.sql import SparkSession

start = time.time()

spark = SparkSession.builder.\
		 config("spark.executor.memory", "2g").appName("query_6").getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile("hdfs://master:9000/files/charts.csv")
artists = sc.textFile("hdfs://master:9000/files/artists.csv")
mapping = sc.textFile("hdfs://master:9000/files/chart_artist_mapping.csv")
regions = sc.textFile("hdfs://master:9000/files/regions.csv")

artists = artists.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0])
mapping = mapping.map(lambda s: tuple(s.split(",")))
regions = regions.map(lambda s: tuple(s.split(",")))

#join to obtaing mapping of id_song and artist
mapping = mapping.map(lambda s: (s[1], s[0])).join(artists).map(lambda s: (s[1][0], s[1][1]))
#filter only Greece!
regions = regions.filter(lambda s: s[1]=="Greece")

rdd = rdd.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0]).\
		filter(lambda s: s[2]=="1" and s[6]=="SAME_POSITION").map(lambda s: (s[4], (s[0], s[5], s[3][:4])))
rdd = regions.join(rdd).map(lambda s: (s[1][1], 1)).reduceByKey(lambda a, b: a+b)
#find max of each year and chart!
max_ = rdd.map(lambda s: ((s[0][1], s[0][2]), s[1])).reduceByKey(lambda a, b: a if a>b else b).map(lambda s: ((s[0][0], s[0][1], s[1]), 0))
#join rdd with max to find all max songs per chart and per year!
rdd = rdd.map(lambda s: ((s[0][1], s[0][2], s[1]), s[0][0]))
rdd = max_.join(rdd).map(lambda s: (s[1][1], s[0])).join(mapping).map(lambda s: (s[1][0][0], s[1][0][1], s[1][1], s[1][0][2])).sortBy(lambda s: (s[0],s[1])).\
			map(lambda s: ','.join(str(d) for d in s))

rdd.coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/rdd_results/result_6")

finish = time.time()

process_time = finish-start

print("Query 6 took:", process_time, "seconds")
