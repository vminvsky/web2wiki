print("====================")
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

import tldextract
import urllib

import sys
import re

from settings import DATA_DIR,EMBEDDING_DIR

print("====================")

"""This function will convert the Wiki shares from several languages to datasets limited to the languages we are interested in studying."""

import sys

import importlib
sys.path.append("/scratch/venia/wiki_embedding_project")
from config.get import cfg
langlist = cfg["langlist"]


os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()

df = spark.read.load(DATA_DIR + "all_wikilinks_clean.parquet")

def run():
    for lang in langlist:
        print(lang)
        temp = df.filter(df.wikidb == lang)
        temp.write.parquet(EMBEDDING_DIR + f"web2wiki/20210201/{lang}/url_mentions.parquet", mode ="overwrite")
        
        

if __name__ == "__main__":
    print("==================")
    run()
