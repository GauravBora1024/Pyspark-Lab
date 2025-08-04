from pyspark import SparkContext, SparkConf

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