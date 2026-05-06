import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback
from configuration import SparkConfig
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

work_path = BASE_DIR / "clean_data" / "data" / "works.parquet"
edition_path = BASE_DIR / "clean_data" / "data" / "editions.parquet"
author_path = BASE_DIR / "clean_data" / "data" / "authors.parquet"

class DataLoader :
    def __init__(self) :
        self.spark = SparkConfig.creat_sparksession()
    def read_works(self) :    
        works = self.spark.read.parquet(str(work_path))
        return works

    def read_editions(self) :    
        editions = self.spark.read.parquet(str(edition_path))
        return editions
    
    def read_authors(self) :        
        authors = self.spark.read.parquet(str(author_path))
        return authors
    

