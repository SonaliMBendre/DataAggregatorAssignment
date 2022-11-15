## Description
This project implements a batch job to read in the input dataset which contains
a metric column with the name of the metric, a value column with the value of the metric, and a
timestamp column. The DataAggregator batch job aggregates the timeseries data by time bucket and metric. 
For each time bucket and each metric in the bucket, it calculates the average value of the metric.

## The commands Line arguments

|       Command   |                                     Description                                      |
|:---------------:|:------------------------------------------------------------------------------------:| 
|  operationType  |                        Aggregate operation (AVG, MIN or MAX)                         | 
| windowDuration  | Timebuckets are created based on windowDuration e.g "2 hours", "24 hours", "2 minutes" etc | 
|   input         |                            file path to the input dataset                            | 
|   output        |           Output dir  to store aggregated values by Timebucket and Metric            | 


## Execution steps

1. Download the project from Github
2. Run maven clean package to package the code into DataAggregator-1.0-SNAPSHOT.jar jar
3. Execute below command to run the code:

   E:\workspace\Projects\DataAggregator> spark-submit --class org.batch.assignment.TimeSeriesDataAggregator .\target\DataAggregator-1.0-SNAPSHOT.jar --operationType AVG --windowDuration "24 hour" --input .\src\main\resources\input  --output .\src\main\resources\output
4. Input : E:\workspace\Projects\DataAggregator\src\main\resources\input (Can be any path on local filesystem)
5. Output :  E:\workspace\Projects\DataAggregator\src\main\resources\output (Can be any path on local filesystem)

## Sample input and output

1. Input : 
   
   a. data.csv

   |Metric|Value|Timestamp|
   |:----:|:---:|:-------:|
   |temperature|88|2022-06-04T12:01:00.000Z|
   |temperature|89|2022-06-04T12:01:30.000Z|
   |temperature|80|2022-06-04T16:01:00.000Z|
   |temperature|92|2022-06-05T14:01:30.000Z|
   |precipitation|0.5|2022-06-04T14:23:32.000Z|
   |precipitation|0.8|2022-06-05T14:23:32.000Z|

   b. data1.csv

   |Metric|Value|Timestamp|
   |:----:|:---:|:-------:|
   |temperature|92|2022-06-04T17:01:30.000Z|
   |temperature|98|2022-06-04T14:01:00.000Z|
   |temperature|99|2022-06-05T14:01:30.000Z|
   |temperature|90|2022-06-05T14:01:00.000Z|
   |precipitation|0.7|2022-06-04T12:01:00.000Z|
   |precipitation|0.3|2022-06-04T12:01:30.000Z|
   |precipitation|0.9|2022-06-04T16:01:00.000Z|
   |precipitation|0.7|2022-06-05T17:01:30.000Z|
   |precipitation|0.5|2022-06-05T14:23:32.000Z|

2. Output

   |Timebucket|            Metric            |      avgValue      |
   |:----------------------------:|:------------------:|:------:|
   |2022-06-04 00:00:00 -> 2022-06-05 00:00:00|        precipitation         |        0.6         |
   |2022-06-04 00:00:00 -> 2022-06-05 00:00:00|         temperature          |        89.4        |
   |2022-06-05 00:00:00 -> 2022-06-06 00:00:00|        precipitation         | 0.6666666666666666 |
   |2022-06-05 00:00:00 -> 2022-06-06 00:00:00| temperature| 93.66666666666667  |

## Prerequisites to run spark job on Windows
1. setup SPARK_HOME, HADOOP_HOME and append it to PATH system env variable
2. HADOOP_HOME --> should point to dir containing winutils.exe, e.g. c:\hadoop\bin\winutils.exe 
3. SPARK_HOME --> spark dir downloaded e.g. c:\spark-3.2.2-bin-hadoop3.2
4. hadoop.dll file should be in c:\windows\system32\
