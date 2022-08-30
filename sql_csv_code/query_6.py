from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from time import time
from functools import reduce

def query6_sql_to_csv():
    spark = SparkSession.builder \
                        .config("spark.ui.port","4041") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .config("spark.task.maxFailures", "8") \
                        .config("spark.default.parallelism", "4") \
                        .appName("query_6").getOrCreate()

    start = time()

    charts = spark.read.csv("hdfs://master:9000/files/charts.csv", header=False, inferSchema=True)

    oldColumns = charts.schema.names
    newColumns = ["song_id", "song", "chart_pos", "date", "country_id", "chart", "move", "streams"]
    charts = reduce(lambda charts, idx: charts.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), charts)
    charts.createOrReplaceTempView("charts")

    chart_artist_mapping = spark.read.csv("hdfs://master:9000/files/chart_artist_mapping.csv", header=False, inferSchema=True)
    mapping = chart_artist_mapping
    oldColumns = mapping.schema.names
    newColumns = ["song_id", "artist_id"]
    mapping = reduce(lambda mapping, idx: mapping.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), mapping)
    mapping.createOrReplaceTempView("mapping")

    artists = spark.read.csv("hdfs://master:9000/files/artists.csv", header=False, inferSchema=True)

    oldColumns = artists.schema.names
    newColumns = ["artist_id", "artist"]
    artists = reduce(lambda artists, idx: artists.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), artists)
    artists.createOrReplaceTempView("artists")

    regions = spark.read.csv("hdfs://master:9000/files/regions.csv", header=False, inferSchema=True)

    oldColumns = regions.schema.names
    newColumns = ["country_id", "country"]
    regions = reduce(lambda regions, idx: regions.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), regions)
    regions.createOrReplaceTempView("regions")

    # steps: 1. Select all columns needed and, for the rows that satinfy the constraints INNER JOIN with regions ON a country id,
    #           to get the name of the country along with other columns
    #        2. For each year, chart and song, count the times a song stayed at position 1
    #        3. For each year and chart, calculate the song with the MAX value of times it stayed at position 1.
    #        4. Apply the MAX trick by recreating the dataframe, and keep the chart, year, the song's id and the maximum days
    #        5. INNER JOIN the mapping with artists, to get a new dataframe with song ids and the corresponding artists
    #        6. INNER JOIN the resulting dataframe with that of step 4 ON the song's id, to get the max value for each chart and year,
    #           along with the artist

    query = """SELECT chart, year, artist, max_days
	        FROM (SELECT d2.chart, d2.year, d1.sg_id1, d2.max_days
                FROM (SELECT chart, year, MAX(cons_d) as max_days
                FROM (SELECT chart, year, song_id as sg_id2, COUNT(*) as cons_d
                FROM (SELECT chart, song_id, year(date) as year, country_id, chart_pos, move
                FROM charts) c2
                INNER JOIN regions r2
                ON c2.country_id=r2.country_id
                WHERE chart_pos='1' AND country='Greece' AND move='SAME_POSITION'
                GROUP BY year, chart, song_id) GROUP BY year, chart ORDER BY year) d2

                INNER JOIN

                (SELECT chart, year, song_id as sg_id1, COUNT(*) as cons_d
                FROM (SELECT chart, song_id, year(date) as year, country_id, chart_pos, move
                FROM charts) c1
                INNER JOIN regions r1
                ON c1.country_id=r1.country_id
                WHERE chart_pos='1' AND country='Greece' AND move='SAME_POSITION'
                GROUP BY year, chart, song_id) d1

                ON max_days=d1.cons_d AND d1.chart=d2.chart AND d1.year=d2.year ORDER BY d2.year, d2.chart) e1

	        INNER JOIN

                (SELECT song_id, artist FROM mapping as map INNER JOIN artists as art ON map.artist_id=art.artist_id) e2

	        ON e1.sg_id1=e2.song_id
		ORDER BY chart, year
             """

    result = spark.sql(query)
    result.show(50)
    result.coalesce(1).write.csv("hdfs://master:9000/outputs/sql_csv_results/result_6", mode="overwrite")

    end = time()
    print("Time needed for execution:", end-start)

    return


if __name__ == "__main__" :
    query6_sql_to_csv()


