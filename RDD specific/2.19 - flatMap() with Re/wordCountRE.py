import re
"""
This script performs a word count on a text file using PySpark.

Functions:
    normalizeWords(text):
        Splits the input text into words by normalizing it to lowercase and 
        splitting on non-word characters using a regular expression.

Workflow:
    1. Initializes a SparkContext with a local master and an application name "wordCountRE".
    2. Reads a text file from the specified path using `sc.textFile`.
    3. Splits the text into words using the `flatMap` transformation and the `normalizeWords` function.
    4. Counts the occurrences of each word using `countByValue`.
    5. Iterates through the word counts and prints each word and its count, ensuring the word is ASCII-encoded.

Dependencies:
    - re: For regular expression operations.
    - pyspark: For distributed data processing.

Note:
    Update the file path in `sc.textFile` to point to the correct location of the input text file.
"""
from pyspark import SparkContext, SparkConf
sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("wordCountRE"))
def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())
input = sc.textFile("D:///Learnings/Pyspark/2.18 - flatMap/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()
for word , count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore').decode('utf-8')
    if cleanWord:
        print(f"{cleanWord} : {count}")