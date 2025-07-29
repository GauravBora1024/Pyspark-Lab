import re
from pyspark import SparkContext, SparkConf
sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("wordCountRE"))
def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())
input = sc.textFile("D:///Learnings/Pyspark/2.18 - flatMap/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountSorted = wordCounts.map(lambda xy: (xy[1], xy[0])).sortByKey()
for result in wordCountSorted.collect():
    count = str(result[0])
    word = result[1].encode('ascii','ignor').decode('utf-8')
    if (word):
        print(f"{word} : {count}")