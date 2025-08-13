'''
UDFs
'''

from pyspark.sql.functions import udf  
from pyspark.sql.types import StringType

#Define a UDF
@udf(returnType = StringType())
def to_uppercase(text):
    return text.upper() if text else None

# Apply to df 
df = df.withColumn("upper_name", to_uppercase(df['name']))




'''
UDTFs
'''


from pyspark.sql.functions import udft
from typing import Iterator, Tuple 
import json 

@udft(returntype = "user_id STRING , event STRING")
def parse_events(json_str: str) -> Iterator[Tuple[str,str]]: # tf is Iterator and Tuple? 
    data = json.loads(json_str)
    user_id = data.get("user", "unknown")
    for event in data.get("events", []):
        yield (user_id , event) # what is yield?



# Register and Use in a Query 
df = spark.sql("select parse_events('\"{\"user\":\"U123\",\"events\":[\"click\",\"view\"]}\"")
df.show()