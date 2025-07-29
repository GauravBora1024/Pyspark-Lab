from pyspark import SparkConf , SparkContext
"""
This script performs a word count operation on a text file using PySpark.
Modules:
    - pyspark: Provides the SparkConf and SparkContext classes for configuring and initializing a Spark application.
Functions:
    - flatMap: Splits each line of the input text file into individual words.
    - countByValue: Counts the occurrences of each unique word in the RDD.
Workflow:
    1. Initializes a SparkContext with a local master and application name "gb".
    2. Reads the input text file located at "D:///Learnings/Pyspark/2.18 - flatMap/book.txt".
    3. Splits the text into words using the `flatMap` transformation.
    4. Counts the occurrences of each word using the `countByValue` action.
    5. Iterates through the word counts and prints each word (after encoding to ASCII and ignoring non-ASCII characters) along with its count.
Note:
    - Ensure that the input file path is correct and accessible.
    - The script filters out non-ASCII characters from the words before printing.
"""

sc = SparkContext(conf = SparkConf().setMaster("local").setAppName("gb"))

input = sc.textFile("D:///Learnings/Pyspark/2.18 - flatMap/book.txt")

words = input.flatMap(lambda x : x.split())

wordCounts =  words.countByValue()

for word,count in wordCounts.items():
    cleanWord = word.encode('ascii','ignore').decode('ascii')
    if (cleanWord):
        print (cleanWord, " " ,count)
