from pyspark.sql import functions as func 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gb").getOrCreate() 

inputDf = spark.read.text(r"D:\gitHub\Pyspark-Lab\DataFrames\WordCount using DFs\book.txt")

words = inputDf.select(func.explode(func.split(inputDf.value,"\\W+")).alias("word")) # why \\W+?

wordsWithoutEmptyString = words.filter(words.word!="") # what?

lowerCaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

wordCounts = lowerCaseWords.groupBy("word").count() # can we use .agg() here?

wordCountedSort = wordCounts.sort("count")


wordCountedSort.show(wordCounts.count()) # why?

spark.stop()