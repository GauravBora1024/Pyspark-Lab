from pyspark import SparkContext, SparkConf
"""
This script processes a CSV file containing customer orders and calculates the total money spent by each customer.
It uses Apache Spark to perform distributed data processing.
Modules:
    - pyspark: Provides SparkContext and SparkConf for initializing and configuring Spark.
Functions:
    - normalizeWords(text): Parses a line of text from the input file, extracts the customer ID and money spent,
      and returns a tuple (customer ID, money spent).
Workflow:
    1. Reads the input CSV file using Spark's `textFile` method.
    2. Maps each line of the file to a tuple (customer ID, money spent) using the `normalizeWords` function.
    3. Aggregates the total money spent by each customer using `reduceByKey`.
    4. Sorts the customers by the total money spent in ascending order.
    5. Collects the results and prints the customer ID and total money spent for each customer.
Input:
    - A CSV file located at "RDD specific\Excercise RDD\customer-orders.csv" with the following format:
      customer_id, order_id, money_spent
Output:
    - Prints the customer ID and total money spent, sorted by the total money spent in ascending order.
Example Output:
    Customer ID: 45, Total Money Spent: $330.50
    Customer ID: 32, Total Money Spent: $450.75
"""

sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("excerciseInvoker"))

input = sc.textFile("RDD specific\Excercise RDD\customer-orders.csv")

def normalizeWords(text):
    fields = text.split(",")
    custid = int(fields[0])
    money = float(fields[2])
    return  (custid, money)

rdd = input.map(normalizeWords)

grouped = rdd.reduceByKey(lambda x, y : x + y)
sortedGroup = grouped.map(lambda x : (x[1],x[0])).sortByKey()
result = sortedGroup.collect()
for i in result:
    print(f"Customer ID: {i[1]}, Total Money Spent: ${i[0]:.2f}")