from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from time import time
from functools import reduce

def query4_sql_to_csv():
    spark = SparkSession.builder \
                        .config("spark.ui.port","4041") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .config("spark.task.maxFailures", "8") \
                        .config("spark.default.parallelism", "4") \
                        .appName("query_4").getOrCreate()

    start = time()

    charts = spark.read.csv("hdfs://master:9000/files/charts.csv", header=False, inferSchema=True)

    oldColumns = charts.schema.names
    newColumns = ["song_id", "song", "chart_pos", "day", "country_id", "chart", "move", "num_streams"]
    charts = reduce(lambda charts, idx: charts.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), charts)
    charts.createOrReplaceTempView("charts")

    regions = spark.read.csv("hdfs://master:9000/files/regions.csv", header=False, inferSchema=True)

    oldColumns = regions.schema.names
    newColumns = ["country_id", "country"]
    regions = reduce(lambda regions, idx: regions.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), regions)
    regions.createOrReplaceTempView("regions")

    # steps: 1. Find for each song in each country the number of times it appears in the required chart
    #        2. Compute which song has the max value and thus, appears most of the times in that chart
    #        3. Apply the INNER JOIN trick for MAX in order to get the song_id
    #	     4. Use the country_id for INNER JOIN with the regions dataframe and get the respective name of the countries

    query = """SELECT r.country, c.song_id, c.sg, c.max as times_in_viral50 FROM ( \
               (SELECT country_id as cnt_id, FIRST_VALUE(song_id) as sg_id, MAX(cnt) as max \
               FROM (SELECT country_id, song_id, COUNT(*) as cnt FROM charts WHERE chart="viral50"
	       GROUP BY country_id, song_id) GROUP BY country_id) c1 \

	       INNER JOIN

	       (SELECT country_id as cnt_id2, song_id, FIRST_VALUE(song) as sg, COUNT(*) as cnt FROM charts WHERE chart="viral50" GROUP BY country_id, song_id) c2 \

               ON c1.max=c2.cnt AND c1.cnt_id=c2.cnt_id2) c \

	       INNER JOIN

               regions r ON r.country_id=c.cnt_id
	       ORDER BY country, sg
            """

    result = spark.sql(query)
    result.show(30)
    result.coalesce(1).write.csv("hdfs://master:9000/outputs/sql_csv_results/result_4", mode="overwrite")

    end = time()
    print("Time needed for execution:", end-start)

    return


if __name__ == "__main__" :
    query4_sql_to_csv()

