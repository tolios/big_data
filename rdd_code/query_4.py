import time
import csv
from pyspark.sql import SparkSession

start = time.time()

spark = SparkSession.builder.appName("query_4").getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile("hdfs://master:9000/files/charts.csv")
regions = sc.textFile("hdfs://master:9000/files/regions.csv")

regions = regions.map(lambda s: tuple(s.split(",")))

rdd = rdd.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0]).\
		filter(lambda s: s[5]=="viral50").map(lambda s: ((s[4], s[0], s[1]), 1)).reduceByKey(lambda a, b: a+b).\
			map(lambda s: (s[0][0], (s[0][1],s[0][2],s[1])))

#keeps only the country and the max!
max_rdd = rdd.reduceByKey(lambda a, b: a if  a[2]>b[2] else b).map(lambda s: ((s[0], s[1][2]), 0))
#to be joined to find more than one who have the max in each country!
rdd = rdd.map(lambda s: ((s[0], s[1][2]), (s[1][0], s[1][1])))
rdd = max_rdd.join(rdd).map(lambda s: (s[0][0], (s[1][1][0], s[1][1][1], s[0][1]))).join(regions).\
			map(lambda s: (s[1][1], s[1][0][0], s[1][0][1], s[1][0][2])).sortBy(lambda s: (s[0], s[2])).\
				map(lambda s: ','.join(str(d) for d in s))

rdd.coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/rdd_results/result_4")

finish = time.time()

process_time = finish-start

print("Query 4 took:", process_time, "seconds")
