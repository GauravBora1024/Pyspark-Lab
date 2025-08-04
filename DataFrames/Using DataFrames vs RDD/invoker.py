from pyspark.sql import SparkSession
"""
This script demonstrates basic DataFrame operations using PySpark.
Functionality:
- Initializes a SparkSession with the application name "gb".
- Reads a CSV file ("fakefriends-header.csv") into a DataFrame with headers and inferred schema.
- Prints the schema of the DataFrame.
- Displays the "name" column from the DataFrame.
- Filters and displays rows where the "age" column is less than 21.
- Groups the DataFrame by "age" and counts the number of records for each age.
- Selects each person's name and their age increased by 10 years.
- Stops the SparkSession after processing.
Filepath: /d:/gitHub/Pyspark-Lab/DataFrames/Using DataFrames vs RDD/invoker.py
"""

spark = SparkSession.builder.appName("gb").getOrCreate()

df = spark.read.option("header" , "true").option("inferschema","true").csv("D:\\gitHub\\Pyspark-Lab\\DataFrames\\Using DataFrames vs RDD\\fakefriends-header.csv")

print("Schema of DataFrame:")
df.printSchema()

print("display the data of name column")
df.select("name").show()

print("filetering using the age column")
df.filter(df.age<21).show()

print("grouping by age and counting the number of friends")
df.groupBy("age").count().show()

print("make everyone 10 yesrs older: ")
df.select(df.name, df.age+10).show()

spark.stop()
