from pyspark import SparkContext, SparkConf
import re
sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("word"))

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("RDD specific/test/book.txt")

words = input.flatMap(normalizeWords)
words_count = words.map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)
Reversed = words_count.map(lambda xy : (xy[1],xy[0])).sortByKey(ascending = False)
# wordsCount = words.countByValue()

for i,j in Reversed.collect():
    cleanword = j.encode('ascii', 'ignore').decode('utf-8')
    if cleanword:
        print(j,i)