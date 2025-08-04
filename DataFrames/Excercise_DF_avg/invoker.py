from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Problem - Firends By Age RDD problem - but now soolve it using dataFrames

spark = SparkSession.builder.appName("gb").getOrCreate()

df = spark.read.option("header" , "true").option("inferschema","true").csv("D:\\gitHub\\Pyspark-Lab\\DataFrames\\Using DataFrames vs RDD\\fakefriends-header.csv")
fieindsByAge = df.select("age", "friends")
fieindsByAge.groupBy("age").avg("friends").sort("age").show()

# using the fucntions module

#formatted more nicecly 
fieindsByAge.groupby("age").agg(func.round(func.avg("friends"),2)).sort("age").show()

# with a custom columns name 
fieindsByAge.groupby("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.stop()