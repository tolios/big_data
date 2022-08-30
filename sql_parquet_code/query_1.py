from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from time import time


def query1_sql_to_parquet():
    spark = SparkSession.builder \
                        .config("spark.ui.port","4041") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .config("spark.task.maxFailures", "8") \
                        .config("spark.default.parallelism", "4") \
                        .appName("query_1").getOrCreate()

    start = time()

    charts = spark.read.parquet("hdfs://master:9000/files/charts.parquet")
    charts.createOrReplaceTempView("charts")

    query = """SELECT SUM(_c7) as number_of_streams \
               FROM charts \
               WHERE _c1='Shape of You' and _c5 = 'top200'
             """

    result = spark.sql(query)
    result.show()
    result.write.csv("hdfs://master:9000/outputs/sql_parquet_results/result_1", mode="overwrite")
    end = time()
    print("Time needed for execution:", end-start)
    return


if __name__ == "__main__":
    query1_sql_to_parquet()


