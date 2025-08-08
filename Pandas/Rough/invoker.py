from pyspark.sql import SparkSession
import pyspark.pandas as pos
import json

spark = SparkSession.builder.appName("gb").getOrCreate()

var = [
  {
    "id": 101,
    "name": "Aarav Mehta",
    "department": "Engineering",
    "salary": 75000,
    "full_time": True,
    "skills": ["Python", "Spark", "AWS"]
  },
  {
    "id": 102,
    "name": "Neha Sharma",
    "department": "Marketing",
    "salary": 58000,
    "full_time": False,
    "skills": ["SEO", "Content Writing"]
  },
  {
    "id": 103,
    "name": "Rohan Gupta",
    "department": "Finance",
    "salary": 67000,
    "full_time": True,
    "skills": ["Excel", "Financial Modeling"]
  },
  {
    "id": 104,
    "name": "Priya Singh",
    "department": "Engineering",
    "salary": 82000,
    "full_time": True,
    "skills": ["Java", "Docker", "Kubernetes"]
  },
  {
    "id": 105,
    "name": "Kabir Verma",
    "department": "HR",
    "salary": 50000,
    "full_time": False,
    "skills": ["Recruitment", "Employee Relations"]
  }
]

rdd = spark.sparkContext.parallelize([json.dumps(row) for row in var])

sdf = spark.read.json(rdd)

pdf = sdf.toPandas()

print(pdf)