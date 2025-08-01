from pyspark.sql import SparkSession
"""
This script demonstrates the use of PySpark to process and analyze data using DataFrames and Spark SQL.
The script performs the following tasks:
1. Creates a SparkSession to initialize the PySpark environment.
2. Defines a `mapper` function to parse lines of a CSV file into Row objects.
3. Reads a CSV file (`fakefriends.csv`) containing data about individuals and their attributes.
4. Maps the CSV data into a DataFrame with an inferred schema and caches it for performance.
5. Registers the DataFrame as a temporary SQL table named "people".
6. Executes an SQL query to filter individuals aged between 13 and 19 (inclusive) and prints the results.
7. Uses PySpark DataFrame functions to group individuals by age, count the occurrences, and display the results in ascending order of age.
8. Stops the SparkSession after processing is complete.
Key Concepts:
- PySpark DataFrames and their schema inference.
- Registering DataFrames as temporary SQL tables.
- Executing SQL queries on DataFrames.
- Using PySpark DataFrame functions for data aggregation and sorting.
Dependencies:
- PySpark library (`pyspark`).
- A CSV file (`fakefriends.csv`) with the following columns: ID, name, age, numFriends.
Note:
- Update the file path for `fakefriends.csv` as per your local environment.
"""
from pyspark.sql import Row

spark = SparkSession.builder.appName("gb").getOrCreate()

def mapper(line):
    fields = line.split(",")
    return Row(ID = int(fields[0]), name = str(fields[1].encode("utf-8")),age = int(fields[2]), numFriends = int(fields[3]))


lines = spark.sparkContext.textFile("D:/gitHub/Pyspark-Lab/DataFrames/SparkSQL-InferSchema/fakefriends.csv")
people = lines.map(mapper)

#infer the schema, and register the dataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over the dataframes that has been registered as tables 
teenagers = spark.sql("select * from people where age >=13 AND age<=19")

# the result of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# we can also use the functions instead of SQL queries 
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()