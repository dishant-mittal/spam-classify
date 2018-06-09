# IOT-Hack

Development of this project involves:</br>
    * Collecting raw data from numerous devices installed in several buildings of University of Waterloo.</br>
    * Crunching the raw timeseries motion sensor data
    * Searching for the most similar users over a complete 5 months/particular day.</br>

As an end result, the client can provide the device-id for a user and search for:</br>
    * The most similar users over complete fall and winter terms.</br>
    * Days on which people depicted similar motion pattern as the queried user.</br>

Dependencies: Python, Java, Scala, Apache Spark, Dygraphs



![Overview](/Overview.PNG)


Notes:
python shows error while importing the pyspark library directly, however , the program will still run.
This is just because these modules are not in the typical python interpreter but spark's python directory
and since spark's directoy itself is not added in configurations. But when you use spark-submit it will work
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

Instead of the above code we can import spark using this:
import findspark #this can be installed directly using pip
