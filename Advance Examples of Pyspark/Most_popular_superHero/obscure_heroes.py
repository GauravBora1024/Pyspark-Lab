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


#random element to check how many counts are there for minimum number 
minConnectioncount = connections.agg(func.min("connections")).first()[0]

mostObscure = connections.filter(func.col("connections")==minConnectioncount)

result_df = mostObscure.alias("obscure").join(names.alias("names"),"id","inner")

print(f"the following characters have only {minConnectioncount} connections")

result_df.select("name").show()

