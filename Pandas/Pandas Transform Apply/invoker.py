from pyspark.sql import SparkSession 
import pyspark.pandas as pos

spark = SparkSession.builder.appName("gb").config("spark.sql.ansi.enabled","false").getOrCreate()

ps_df =  pos.DataFrame({
    "id" : [1,2,3,4,5], 
    "name" : ["Alice", "Bob" , "Charlie" , "David" , "Emma"],
    "age" :  [25,30,35,40,45]
    , "salary": [50000, 60000, 75000, 80000, 120000]
})


print("OG pandas on pysaprk dataframe Print")
print(ps_df)

#  using the transform() for elemnt wise operations 
ps_df['age_in_10_years'] = ps_df["age"].transform(lambda x : x+10)
print("Dataframe after the transformation (age+10 years)")
print(ps_df)


#  using the apply on columns 
# Define a custom functions to categorize salary levels 
def categorize_salary(salary):
    if salary < 60000:
        return "Low"
    elif salary < 100000:
        return "medium"
    else: 
        return "High"

#  apply the functtion to salary column
ps_df["salary_category"] = ps_df["salary"].apply(categorize_salary)
print("Dataframe after Apply function: ")
print(ps_df)

# using the apply() on rows - 
def format_row(row):
    return f"{row['name']} ({row['age']} years old)"

ps_df["Name_with_age"] = ps_df.apply(format_row, axis = 1)
print("Pandas DataFRame apply() on rows")
print(ps_df)

spark.stop()



'''
   OG pandas on pysaprk dataframe Print
   id     name  age  salary
0   1    Alice   25   50000
1   2      Bob   30   60000
2   3  Charlie   35   75000
3   4    David   40   80000
4   5     Emma   45  120000

Dataframe after the transformation (age+10 years)
   id     name  age  salary  age_in_10_years
0   1    Alice   25   50000               35
1   2      Bob   30   60000               40
2   3  Charlie   35   75000               45
3   4    David   40   80000               50
4   5     Emma   45  120000               55

Dataframe after Apply function: 
   id     name  age  salary  age_in_10_years salary_category
0   1    Alice   25   50000               35             Low
1   2      Bob   30   60000               40          medium
2   3  Charlie   35   75000               45          medium
3   4    David   40   80000               50          medium
4   5     Emma   45  120000               55            High

C:\Users\gaura\AppData\Local\Programs\Python\Python310\lib\site-packages\pyspark\pandas\utils.py:1037: PandasAPIOnSparkAdviceWarning: If the type hints is not specified for `apply`, it is expensive to infer the data type internally.
  warnings.warn(message, PandasAPIOnSparkAdviceWarning)
  
Pandas DataFRame apply() on rows
   id     name  age  salary  age_in_10_years salary_category           Name_with_age
0   1    Alice   25   50000               35             Low    Alice (25 years old)
1   2      Bob   30   60000               40          medium      Bob (30 years old)
2   3  Charlie   35   75000               45          medium  Charlie (35 years old)
3   4    David   40   80000               50          medium    David (40 years old)
4   5     Emma   45  120000               55            High     Emma (45 years old)
'''