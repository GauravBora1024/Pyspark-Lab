import re
"""
This script performs a word count on a text file and sorts the results by the count in ascending order.

Functions:
    normalizeWords(text): Splits the input text into lowercase words using a regular expression to remove non-word characters.

Workflow:
1. Reads a text file using SparkContext.
2. Normalizes the text into lowercase words using the `normalizeWords` function.
3. Maps each word to a key-value pair (word, 1).
4. Reduces the key-value pairs by key to calculate the total count for each word.
5. Maps the word count pairs to (count, word) format for sorting.
6. Sorts the word count pairs by the count in ascending order.
7. Collects and prints the sorted word counts, ensuring proper encoding for display.

Dependencies:
    - re: For regular expression operations.
    - pyspark: For distributed data processing.

Input:
    - A text file located at "D:///Learnings/Pyspark/2.18 - flatMap/book.txt".

Output:
    - Prints each word and its count in ascending order of the count.
"""
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