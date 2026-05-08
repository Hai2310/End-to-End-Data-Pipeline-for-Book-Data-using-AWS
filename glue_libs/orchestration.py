import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback
from pathlib import Path
from load_data import DataLoader 
from elt_data import EtlPipeline
from clean_data import DataClean

# BASE_DIR = Path(__file__).resolve().parent.parent
# DIR = BASE_DIR / "data_warehouse" 

class BookPipeline :
    def __init__(self):
        self.spark = SparkConfig.create_sparksession()
        self.load = DataLoader()
        self.clean = DataClean
        self.etl = EtlPipeline
        self.result = {}
    def load_data(self) :
        print("\n" + "="*60 )
        print("====== Data Loading Phase ======")
        print("="*60 )

        self.work = self.clean.work_clean(self.load.read_works())
        self.edition = self.clean.edition_clean(self.load.read_editions())
        self.author = self.clean.author_clean(self.load.read_authors())

        print("\n All data loaded successfully")

    def run_etl_1(self) :
        try :
            result = self.etl.dim_work(self.work)
            self.result["dim_work"] = result
            return result
        except Exception as e :
            print("Error in ETL 1 : " , str(e))
            raise
    
    def run_etl_2(self) :
        try :
            result = self.etl.dim_subject(self.work)
            self.result["dim_subject"] = result
            return result
        except Exception as e :
            print("Error in ETL 2 : " , str(e))
            raise
    
    def run_etl_3(self) :
        try :
            result = self.etl.work_subject(self.work , self.result["dim_subject"])
            self.result["work_subject"] = result
            return result
        except Exception as e :
            print("Error in ETL 3 : " , str(e))
            raise
    
    def run_etl_4(self) :
        try :
            result = self.etl.work_author(self.work)
            self.result["work_author"] = result
            return result
        except Exception as e :
            print("Error in ETL 4 : " , str(e))
            raise
    
    def run_etl_5(self) :
        try :
            result = self.etl.dim_edition(self.edition)
            self.result["dim_edition"] = result
            return result
        except Exception as e :
            print("Error in ETL 5 : " , str(e))
            raise
    
    def run_etl_6(self) :
        try :
            result = self.etl.dim_time(self.edition)
            self.result["dim_time"] = result
            return result
        except Exception as e :
            print("Error in ETL 6 : " , str(e))
            raise
    
    def run_etl_7(self) :
        try :
            result = self.etl.dim_author(self.author)
            self.result["dim_author"] = result
            return result
        except Exception as e :
            print("Error in ETL 7 : " , str(e))
            raise
    
    def run_etl_8(self) :
        try :
            result = self.etl.fact_book(self.edition, self.result["dim_time"])
            self.result["fact_book"] = result
            return result
        except Exception as e :
            print("Error in ETL 8 : " , str(e))
            raise
    
    def run_all_etl(self) :
        print("\n" + "="*60 )
        print("====== ETL PHASE - RUNNING ALL ETL ======")
        print("="*60 )

        total_start = time.time()

        self.run_etl_1()
        self.run_etl_2()
        self.run_etl_3()
        self.run_etl_4()
        self.run_etl_5()
        self.run_etl_6()
        self.run_etl_7()
        self.run_etl_8()

        total_elapsed = time.time() - total_start

        print("\n" + "="*60 )
        print(f"====== ALL ETL IN COMPLETED IN {total_elapsed:.2f}s ======")
        print("="*60 )
    
    def save_parquet(self) :
        print("\n" + "="*60 )
        print("====== STARTING SAVE TABLE ======")
        print("="*60 )

        for name, df in self.result.items() :
            df_partition = df.withColumn("year" , year(current_date())) \
                             .withColumn("month" , month(current_date())) \
                             .withColumn("day" , day(current_date()))
            
            df_partition.write \
              .partitionBy("year", "month", "day") \
              .mode("overwrite") \
              .parquet(f"s3://mhai-bk/data_warehouse/{name}")
    
    def spark_stop(self) :
        self.spark.stop()
        print("=== Spark Session Stop ===")