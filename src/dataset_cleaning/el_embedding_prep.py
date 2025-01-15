import enum
import pandas as pd 
import os

import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyarrow.parquet as pq
import pyarrow as pa
import glob

import sys
sys.setrecursionlimit(10000)

import re
import time
from bs4 import BeautifulSoup

sys.path.append("/dlabdata1/web2wiki/web2wiki/")
from settings import WIKI_PAGES_DIR

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()

@F.udf
def process_row(x):
    pass
    

def main():
    files = glob.glob("/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/wiki_content_*")
    
    df = spark.read.load(files)
    
    hypertext_url_count = df.groupby("hypertext", "wiki_url").count()

    hypertext_url_count.write.parquet("/dlabdata1/web2wiki/web2wiki/data/el/hypertext_wiki_pairs.parquet", mode="overwrite")
    

if __name__=="__main__":
    main()
