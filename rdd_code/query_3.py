import time
import csv
from pyspark.sql import SparkSession

start = time.time()

spark = SparkSession.builder.appName("query_3").getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile("hdfs://master:9000/files/charts.csv")
#filters top200 and 1st chart position
rdd = rdd.map(lambda s : list(csv.reader([s], delimiter=',', quotechar='"'))[0]).\
				filter(lambda s: s[5] == "top200").filter(lambda s: s[2]=="1")
#maps to key, value pair, where key is a tuple of year, month and day, with value as the streams, then finds total daily streams!
rdd = rdd.map(lambda s : ((s[3][0:4], s[3][5:7], s[3][8:10]), int(s[7]))).reduceByKey(lambda a, b: a+b)
#finally finds the monthly avg and sorts in ascending order of month and year!
rdd = rdd.map(lambda s: ((s[0][0], s[0][1]), (s[1], 1))).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda v: v[0]/v[1]).\
				map(lambda s: (s[0][0], int(s[0][1]) , s[1])).sortBy(lambda s: (int(s[0]), int(s[1]))).\
					map(lambda s: ','.join(str(d) for d in s))

rdd.coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/rdd_results/result_3")

finish = time.time()

process_time = finish-start

print("Query 3 took:", process_time, "seconds")
