from pyspark.sql import SparkSession
"""
This script demonstrates the use of PySpark's User Defined Functions (UDFs) 
in conjunction with Spark SQL to manipulate data in a DataFrame.
The script performs the following steps:
1. Creates a SparkSession.
2. Reads a JSON file into a DataFrame.
3. Registers the DataFrame as a temporary SQL view.
4. Defines a Python function `add_age` that adds 10 to a given number.
5. Registers the `add_age` function as a UDF with Spark, specifying the return type as IntegerType.
6. Uses Spark SQL to apply the `add_age` UDF to the `age` column of the DataFrame, filtering out rows where `age` is null.
7. Displays the resulting DataFrame.
Dependencies:
- PySpark library
- A JSON file located at "DataFrames\SparkSQL\code.json" containing the data to be processed.
Note:
Ensure that the JSON file path is correct and accessible before running the script.
"""
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("gb").getOrCreate()

df = spark.read.json("DataFrames\SparkSQL\code.json")
df.createOrReplaceTempView("df_new")

# createing the udf
def add_age(num):
    return num + 10

spark.udf.register("add_age", add_age , IntegerType())

df_new = spark.sql("select add_age(age) from df_new where age is not null")
df_new.show()


