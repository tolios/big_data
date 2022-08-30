from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from time import time
from functools import reduce

def query3_sql_to_csv():
    spark = SparkSession.builder \
                        .config("spark.ui.port","4041") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .config("spark.task.maxFailures", "8") \
                        .config("spark.default.parallelism", "4") \
                        .appName("query_3").getOrCreate()

    start = time()

    charts = spark.read.csv("hdfs://master:9000/files/charts.csv", header=False, inferSchema=True)

    oldColumns = charts.schema.names
    newColumns = ["id", "song", "chart_pos", "date", "country_id", "chart", "move", "num_streams"]
    charts = reduce(lambda charts, idx: charts.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), charts)
    charts.createOrReplaceTempView("charts")

    # steps: 1. convert date to year and month and select other useful characteristics for the rows that we need
    #        2. Group by year and month, in order to calculate the sum of streams and the number of days of each month
    #        3. Return all required values, divide the number of streams with the days of each month to get the average
    query = """SELECT year, month, sum_streams/days_of_month as mean_number_of_daily_streams_in_No1 \
               FROM (SELECT year, month, COUNT(DISTINCT day) as days_of_month, SUM(num_streams) as sum_streams \
               FROM (SELECT id, year(date) as year, month(date) as month, day(date) as day, chart_pos, num_streams  \
               FROM charts \
               WHERE chart="top200" and chart_pos=1) \
               GROUP BY year, month ORDER BY year, month)\
            """
    result = spark.sql(query)
    result.show(5)
    result.coalesce(1).write.csv("hdfs://master:9000/outputs/sql_csv_results/result_3", mode="overwrite")

    end = time()
    print("Time needed for execution:", end-start)

    return


if __name__ == "__main__" :
    query3_sql_to_csv()

