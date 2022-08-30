from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from time import time
from functools import reduce

def query5_sql_to_parquet():
    spark = SparkSession.builder \
                        .config("spark.ui.port","4041") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .config("spark.task.maxFailures", "8") \
                        .config("spark.default.parallelism", "4") \
                        .appName("query_5").getOrCreate()

    start = time()

    charts = spark.read.parquet("hdfs://master:9000/files/charts.parquet")

    oldColumns = charts.schema.names
    newColumns = ["song_id", "song", "chart_pos", "date", "country_id", "chart", "move", "num_streams"]
    charts = reduce(lambda charts, idx: charts.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), charts)
    charts.createOrReplaceTempView("charts")

    chart_artist_mapping = spark.read.parquet("hdfs://master:9000/files/chart_artist_mapping.parquet")
    mapping = chart_artist_mapping
    oldColumns = mapping.schema.names
    newColumns = ["song_id", "artist_id"]
    mapping = reduce(lambda mapping, idx: mapping.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), mapping)
    mapping.createOrReplaceTempView("mapping")

    artists = spark.read.parquet("hdfs://master:9000/files/artists.parquet")

    oldColumns = artists.schema.names
    newColumns = ["artist_id", "artist"]
    artists = reduce(lambda artists, idx: artists.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), artists)
    artists.createOrReplaceTempView("artists")


    query = """SELECT year, artist as artist_name, CAST(best_average_streams AS DECIMAL(14, 6)) \

	       FROM (SELECT d1.year, d2.art_id, max_avg as best_average_streams \

               FROM (SELECT year, MAX(avg) as max_avg \
	       FROM (SELECT year(date) as year, SUM(num_streams)/69 as avg, map1.artist_id as artist_id\
               FROM charts as c1\
               INNER JOIN mapping as map1 ON map1.song_id=c1.song_id WHERE c1.chart='top200' \
	       GROUP BY year, map1.artist_id) \
	       GROUP BY year) d1\

	       INNER JOIN

	       (SELECT year(date) as yr, SUM(num_streams)/69 as avg2, map2.artist_id as art_id \
	       FROM charts as c2 \
               INNER JOIN mapping as map2 ON map2.song_id=c2.song_id \
               WHERE c2.chart='top200' \
               GROUP BY year(date), map2.artist_id) d2 \

	       ON d1.max_avg=d2.avg2) as f \

               INNER JOIN artists art ON art.artist_id=f.art_id ORDER BY year
            """

    result = spark.sql(query)
    result.show(5)
    result.coalesce(1).write.csv("hdfs://master:9000/outputs/sql_parquet_results/result_5", mode="overwrite")

    end = time()
    print("Time needed for execution:", end-start)

    return


if __name__ == "__main__" :
    query5_sql_to_parquet()

