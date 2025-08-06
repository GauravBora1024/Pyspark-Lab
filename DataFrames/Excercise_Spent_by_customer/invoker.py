from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType 


spark = SparkSession.builder.appName("customer_spending").getOrCreate()

schema = StructType([
    StructField("customer_id",IntegerType(),True),\
    StructField("item_id",IntegerType(),True),\
    StructField("amount",FloatType(),True)
])

df = spark.read.schema(schema).csv(r"D:\gitHub\Pyspark-Lab\DataFrames\Excercise_Spent_by_customer\customer-orders.csv")

grouped = df.groupBy("customer_id").agg(func.round(func.sum("amount"),2).alias("amount_spent")).sort("amount_spent")

result = grouped.collect()

for row in result:
    print(row[0],"\t{:.2f}".format(row[1]))


# OR USE FOR PRINTING --> result.show(result.count())


spark.stop()