from pyspark.sql import SparkSession


spark = SparkSession.builder \
                    .config("spark.ui.port","4041") \
                    .config("spark.executor.memory", "2g") \
                    .config("spark.executor.cores", "2") \
                    .config("spark.task.maxFailures", "8") \
                    .config("spark.default.parallelism", "4") \
                    .appName("csv_to_parquet").getOrCreate()


charts = spark.read.csv("hdfs://master:9000/files/charts.csv", header=False, inferSchema=True)
charts.write.parquet("hdfs://master:9000/files/charts.parquet")

chart_artist_mapping = spark.read.csv("hdfs://master:9000/files/chart_artist_mapping.csv", header=False, inferSchema=True)
chart_artist_mapping.write.parquet("hdfs://master:9000/files/chart_artist_mapping.parquet")

artists = spark.read.csv("hdfs://master:9000/files/artists.csv", header=False, inferSchema=True)
artists.write.parquet("hdfs://master:9000/files/artists.parquet")

regions = spark.read.csv("hdfs://master:9000/files/regions.csv", header=False, inferSchema=True)
regions.write.parquet("hdfs://master:9000/files/regions.parquet")


