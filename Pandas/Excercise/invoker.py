from pyspark.sql import SparkSession 

# must set this env varibale to avoid any warnings 
import os 
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pandas as pd 
import pyspark.pandas as ps 

spark = SparkSession.builder \
.appName("gaurav") \
.config("spark.sql.ansi.enabled","false") \
.config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
.getOrCreate()



pdf = pd.DataFrame({
    "id":[1,2,3,4,5],
    "name": ["Alice", "Bobn", "charlie" , "david" , "Emma"],
    "Age" :  [25,30,35,40,45]
})

print("OG Pandas DataFrame: ")
print(pdf)

# convert the Pandas DataFrame to Spark DataFrame 
sdf  = spark.createDataFrame(pdf)

print("the Schema of the SDF  : ")
sdf.printSchema()

print("printTing the converted SDF from PDF")
sdf.show()

# performing the transformations on the spark daataframe - 
filtered_spark_df =  sdf.filter("age>30")
# filtered_spark_df =  sdf.filter(sdf.age>30)
filtered_spark_df.show()


converted_pdf = filtered_spark_df.toPandas()
print("printing converted pdf from sdf")
print(converted_pdf)

# use pandas-on-spark for Scalable Pandas operations 
pos_df = ps.DataFrame(converted_pdf)

# performing the pandas like operations on this dataFrame 
print("Using the pandas-On-spark transformation similar as pdf transformations")
pos_df['Age'] = pos_df['Age'] + 1 


# pos to sdf 
converted_sdf = pos_df.to_spark()
print("the sdf converted from pos_df")
converted_sdf.show()

spark.stop()



'''
RESULT - 

OG Pandas DataFrame: 
   id     name  Age
0   1    Alice   25
1   2     Bobn   30
2   3  charlie   35
3   4    david   40
4   5     Emma   45
the Schema of the SDF  : 
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- Age: long (nullable = true)

printTing the converted SDF from PDF
+---+-------+---+
| id|   name|Age|
+---+-------+---+
|  1|  Alice| 25|
|  2|   Bobn| 30|
|  3|charlie| 35|
|  4|  david| 40|
|  5|   Emma| 45|
+---+-------+---+

+---+-------+---+
| id|   name|Age|
+---+-------+---+
|  3|charlie| 35|
|  4|  david| 40|
|  5|   Emma| 45|
+---+-------+---+

printing converted pdf from sdf
   id     name  Age
0   3  charlie   35
1   4    david   40
2   5     Emma   45
Using the pandas-On-spark transformation similar as pdf transformations
C:\Users\gaura\AppData\Local\Programs\Python\Python310\lib\site-packages\pyspark\pandas\utils.py:1037: PandasAPIOnSparkAdviceWarning: If `index_col` is not specified for `to_spark`, the existing index is lost when converting to Spark DataFrame.
  warnings.warn(message, PandasAPIOnSparkAdviceWarning)
the sdf converted from pos_df
+---+-------+---+
| id|   name|Age|
+---+-------+---+
|  3|charlie| 36|
|  4|  david| 41|
|  5|   Emma| 46|
+---+-------+---+



'''