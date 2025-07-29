'''
"""
This script processes weather data to find the minimum temperature recorded at each weather station.
The input data is expected to be in CSV format with the following fields:
    weather-station-id, date, observation-type, temperature, unused, unused, E, unused
Example input data:
The script performs the following steps:
1. Reads the input data from a CSV file using PySpark.
2. Parses each line of the input data to extract the station ID, observation type, and temperature.
3. Converts the temperature from tenths of degrees Celsius to Fahrenheit.
4. Filters the data to include only "TMIN" (minimum temperature) observation types.
5. Maps the data to key-value pairs of station ID and temperature.
6. Reduces the data by key to find the minimum temperature for each station.
7. Collects the results and prints the minimum temperature for each station in Fahrenheit.
Output Example:
Dependencies:
    - PySpark: Requires PySpark to be installed and configured.
Note:
    - Update the file path in `sc.textFile()` to point to the correct location of the input CSV file.
    - The script assumes the input data is well-formed and does not handle malformed lines.
"""
weather-station-id , date , observation-type , temperature,unused,unused,E,unused
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
EZE00100082,18000101,TMIN,-135,,,E,
'''

# --------------- CODE is BELOW --------------------------

from pyspark import SparkContext, SparkConf
sc = SparkContext(conf = SparkConf().setMaster("local").setAppName("gb"))
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])*0.1*(9.0/5.0)+32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("D:///Learnings/Pyspark/2.15/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x : "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0],x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y : min(x,y))
result = minTemps.collect()
for i in result:
    print(f"for station {i[0]} minimum temperature is {round(i[1],2)}F")
sc.stop()

#------------------------ code ended---------------------------
''' output -->
for station ITE00100554 minimum temperature is 5.36F
for station EZE00100082 minimum temperature is 7.7F
'''