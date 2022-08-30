import time
import csv
from pyspark.sql import SparkSession

start = time.time()

spark = SparkSession.builder.appName("query_2").getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile("hdfs://master:9000/files/charts.csv")

rdd = rdd.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0]).\
	filter(lambda s: s[2] == "1").map(lambda s: ((s[5], s[1]), 1/69)).reduceByKey(lambda a, b: a+b).\
		map(lambda s: (s[0][0], (s[0][1], s[1]))).reduceByKey(lambda a, b: a if a[1] > b[1] else b).map(lambda s: (s[0], s[1][0], s[1][1])).\
			map(lambda s: ','.join(str(d) for d in s))
rdd.coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/rdd_results/result_2")

finish = time.time()

process_time = finish-start

print("Query 2 took:", process_time, "seconds")
