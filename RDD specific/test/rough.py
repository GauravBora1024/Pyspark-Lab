from pyspark import SparkContext, SparkConf


sc = SparkContext(conf = SparkConf().setMaster("local").setAppName("abc"))

lines = sc.parallelize([1,2,3,4,5])

rd = lines.map(lambda x  : (x,x))
rd1 = rd.mapValues(lambda x  : (x,1))

a = rd1.collect()
print(a)