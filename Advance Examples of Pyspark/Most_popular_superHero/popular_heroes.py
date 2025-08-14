from pyspark.sql import SparkSession 
from pyspark.sql import functions as func
from pyspark.sql.types import StructType , StructField , IntegerType , StringType

spark = SparkSession.builder.appName("heroes").getOrCreate()

schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True)
])


names = spark.read.schema(schema=schema).option("sep"," ").csv(r"D:\gitHub\Pyspark-Lab\Advance Examples of Pyspark\Most_popular_superHero\Marvel+Names.txt")

lines = spark.read.text(r"D:\gitHub\Pyspark-Lab\Advance Examples of Pyspark\Most_popular_superHero\Marvel+Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
                .withColumn("connections",func.size(func.split(func.col("value")," ")) - 1) \
                .groupBy("id").agg(func.sum("connections").alias("connections"))



mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is most popular superhero with "+str(mostPopular[1])+" Counts")
