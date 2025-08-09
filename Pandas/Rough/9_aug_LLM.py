from pyspark.sql import SparkSession
import pyspark.pandas as ps

spark= SparkSession.builder.config("spark.sql.ansi.enabled","false").appName("gb").getOrCreate()


data = [
    (1, " alice ", 29),
    (2, "Bob", 31),
    (3, " Cathy", 25),
    (4, "david ", 35),
    (5, None, 28)
]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)


pos = df.pandas_api()

pos['name'] = pos['name'].transform(lambda x : x.strip() if isinstance(x,str) else x)
pos['name'] = pos['name'].transform(lambda x : x.capitalize() if isinstance(x,str) else x)
pos['name'] = pos['name'].transform(lambda x : "Unknown" if x is None else x)

print(pos)