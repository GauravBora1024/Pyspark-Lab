from pyspark import SparkContext, SparkConf
"""
This script calculates the average number of friends by age from a CSV file using PySpark RDDs.
Workflow:
1. Initializes a SparkContext with a local master.
2. Defines a function `parseLine` to parse each line of the CSV and extract age and number of friends.
3. Reads the CSV file, removes the header, and parses each line into (age, numFriends) tuples.
4. Maps each value to a (sum, count) tuple, then reduces by key (age) to aggregate total friends and counts.
5. Computes the average number of friends for each age.
6. Prints the results as (age, average number of friends) tuples.
Assumptions:
- The input CSV file has a header and is comma-separated.
- The age is in the third column (index 2), and the number of friends is in the fourth column (index 3).
"""

spark = SparkContext(conf = SparkConf().setAppName("gb").setMaster("local"))

def parseLine(Line):
    fields = Line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = spark.textFile("D:\\gitHub\\Pyspark-Lab\\DataFrames\\Using DataFrames vs RDD\\fakefriends-header.csv")

header = lines.first()
lines = lines.filter(lambda x : x!=header)

rdd = lines.map(parseLine)

grouped = rdd.mapValues(lambda x :(x,1)).reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1]))
average = grouped.mapValues(lambda x : x[0]/x[1])
for i in average.collect():
    print(i)