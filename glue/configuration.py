import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback

class SparkConfig :
    def creat_sparksession() :
        spark = SparkSession.builder.config("spark.driver.memory" , "8g") \
                            .appName("clean data") \
                            .master("local[*]") \
                            .getOrCreate()
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "file:///")
        return spark