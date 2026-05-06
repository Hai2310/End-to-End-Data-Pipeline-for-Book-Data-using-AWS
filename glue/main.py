import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback
from configuration import SparkConfig
from load_data import DataLoader 
from elt_data import EtlPipeline
from clean_data import DataClean
from orchestration import BookPipeline

def main() :
    print("\n" + "="*60 )
    print("====== BOOK ANALYSIS SYSTEM ======")
    print("="*60 )

    try :
        Pipeline = BookPipeline()

        Pipeline.load_data()
        Pipeline.run_all_etl()
        Pipeline.save_parquet()

        print("\n" + "="*60 )
        print("====== ALL PIPELINE COMPLETED ======")
        print("="*60 )
    
    except Exception as e :
        print("Pipeline failed : ", str(e))
        traceback.print_exc()

if __name__ == "__main__" :
    main()