from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("gb").getOrCreate()


Schema = StructType([\
    StructField("stationId",StringType(), True),\
    StructField("date",IntegerType(), True),\
    StructField("measure_type",StringType(), True),\
    StructField("temperature",FloatType(), True)
]) 

df = spark.read.schema(Schema).csv(r'D:\gitHub\Pyspark-Lab\DataFrames\min_max_temperature_by_df\1800.csv')


stationTemps = df.filter(df.measure_type == "TMIN").select("stationId","temperature")

minTempByStation = stationTemps.groupBy("stationId").min("temperature")

# convert the temperature to fahrenheit and sort the dataSet
minTempsByStationF = minTempByStation.withColumn("temperature", func.round(func.col("min(temperature)")*0.1*(9/5)+32,2)).select("stationId","temperature").sort("temperature")

results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()



'''
result = 

ITE00100554     5.36F
EZE00100082     7.70F

'''