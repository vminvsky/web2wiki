from dataset_cleaning.extraction_helpers import * 
import enum
from http.client import responses
import pandas as pd 
import os
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import glob
import sys
from settings import *
    
sys.path.append(os.path.join(MAIN_DIR, "/code/helpers/"))
sys.path.append(os.path.join(MAIN_DIR,"/code/iterative_coding/"))

import re
import time

import helping_functions as hf
from dotenv import load_dotenv
load_dotenv()


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    iteration = "0"
    files = glob.glob(WIKI_PAGES_DIR+"/*")
    print("==================")

    files_partitioned = chunks(files, 64)
    for i, file_paths in enumerate(files_partitioned):
        try:
            pd.Series(files).to_csv(DATA_DIR+f"iterated_coding/temp_files/files_{iteration}_{i}.csv")

        #     print(file_paths)
            print("==================")
            print(f"We are on partition {i}")
            t = time.time()
            df = spark.read.load(file_paths)
            df = df.withColumn("processed", hf.process_all_udf("content"))
            df = df.drop(F.col("content"))

            df.write.parquet(f"/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/tag_info_{iteration}_{i}.parquet", mode = "overwrite")
            print("It took {:.2f} seconds to write one chunk.".format(time.time() - t))
            df.unpersist()
            del df
        except: 
            print(f"Iteration {i} broke down, skipping to next iteration.")
