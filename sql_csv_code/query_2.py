from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from time import time
from functools import reduce

def query2_sql_to_csv():
    spark = SparkSession.builder \
                        .config("spark.ui.port","4041") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .config("spark.task.maxFailures", "8") \
                        .config("spark.default.parallelism", "4") \
                        .appName("query_2").getOrCreate()

    start = time()

    charts = spark.read.csv("hdfs://master:9000/files/charts.csv", header=False, inferSchema=True)

    oldColumns = charts.schema.names
    newColumns = ["id", "song", "chart_pos", "day", "country_id", "chart", "move", "num_streams"]
    charts = reduce(lambda charts, idx: charts.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), charts)
    charts.createOrReplaceTempView("charts")

    # steps: 1. For each song in each chart, sum chart_pos for all countries for those rows that satisfy the constraints
    #        2. Divide by 69 in order to get the average
    #        3. Find the song with the max average value for each chart
    #        4. Apply trick with INNER JOIN in order to access more columns of the dataframe, without applying GROUP BY
    query = """SELECT c1.chart, song, mean_time_in_pos_1 \

               FROM (SELECT chart, MAX(avg) as mean_time_in_pos_1 \
               FROM (SELECT chart, song, SUM(chart_pos)/69 as avg \
               FROM (SELECT chart, song, chart_pos FROM charts WHERE chart_pos=1) GROUP BY chart, song) \
               GROUP BY chart) as c1

               INNER JOIN

	       (SELECT chart, song, SUM(chart_pos)/69 as avg \
               FROM (SELECT chart, song, chart_pos FROM charts WHERE chart_pos=1) GROUP BY chart, song) c2 \

	       ON c1.mean_time_in_pos_1=c2.avg
               ORDER BY chart DESC, mean_time_in_pos_1 DESC
            """

    result = spark.sql(query)
    result.show(2)
    result.coalesce(1).write.csv("hdfs://master:9000/outputs/sql_csv_results/result_2", mode="overwrite")

    end = time()
    print("Time needed for execution:", end-start)

    return


if __name__ == "__main__" :
    query2_sql_to_csv()
