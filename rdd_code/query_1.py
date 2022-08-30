import time
import csv
from pyspark.sql import SparkSession

start = time.time()

spark = SparkSession.builder.appName("query_1").getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile("hdfs://master:9000/files/charts.csv")

rdd = rdd.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0]).\
		filter(lambda s: s[1] == "Shape of You").filter(lambda s: s[5]=="top200").map(lambda s: ("result", int(s[7]))).\
			reduceByKey(lambda a, b: a+b).map(lambda s: s[1])

rdd.coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/rdd_results/result_1")

finish = time.time()

process_time = finish-start

print("Query 1 took:", process_time, "seconds")
