from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField , IntegerType, LongType

spark = SparkSession.builder.appName("movies_advance").getOrCreate()

schema = StructType([
    StructField("userid",IntegerType(),True),
    StructField("movieid",IntegerType(),True),
    StructField("rating",IntegerType(),True),
    StructField("timestamp", LongType(),True)
])

df = spark.read.csv(r"D:\gitHub\Pyspark-Lab\Advance Examples of Pyspark\Most_popular_movie\movie.csv", schema=schema)

topMoviesId = df.groupBy("movieId").agg(F.count("*").alias("count")).orderBy(F.desc("count"))

topMoviesId.show(2)

spark.stop()