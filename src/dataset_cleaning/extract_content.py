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
import numpy as np

import sys
sys.path.append("/dlabdata1/web2wiki/web2wiki/")
sys.setrecursionlimit(10000)

import re
import time
from bs4 import BeautifulSoup

from settings import WIKI_PAGES_DIR

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()

files = glob.glob(WIKI_PAGES_DIR+"/*")[1000:]

schema = StructType([
    StructField("a1", ArrayType(StringType()), False),
    StructField("a2", ArrayType(StringType()), False),
    StructField("hypertexts", ArrayType(StringType()),False),
    StructField("raw_text", ArrayType(StringType()),False),
    StructField("wiki_url", ArrayType(StringType()), False),
    StructField("text1", ArrayType(StringType()),False),
    StructField("text2", ArrayType(StringType()), False)
])

regex_search = r"(en.wikipedia\.org[\s\/a-zA-ZäöüÄÖÜßþóúí\_\?(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:\!]+)"

@F.udf
def extract_regex(x):
    links = re.findall(regex_search, x)
    return links

def extract_soup(x: str, only_para = False):
    soup = BeautifulSoup(x,features="lxml")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
    
    hypertexts = []
    raw_text = []
    k1 = []
    k2 = []
    p_texts1 = []
    p_texts2 = []
    
    k_name = []
    
    for a in all_links:
        text = a.text
        if a.get("href") != None:
            parent = a.parent
            
            k1.append(str(parent.name))
            k2.append(str(parent.parent.name))
            
            if only_para == True:
                if parent.name == "p":
                    p_texts1.append(str(parent))
                    raw_text.append(parent.text)
                else:
                    p_texts1.append(None)
                    raw_text.append(None)
                if parent.parent.name == "p":
                    p_texts2.append(str(parent.parent))
                else:
                    p_texts2.append(None)
            
            else: 
                try:
                    p_texts1.append(str(parent))
                    raw_text.append(parent.text)
                    p_texts2.append(str(parent.parent))
                except:
                    print("Didn't work")
                    raw_text.append(None)
                    p_texts1.append(None)
                    p_texts2.append(None)
            hypertexts.append(text)
            k_name.append(a.get("href"))
    return [k1,k2, hypertexts, raw_text, k_name, p_texts1, p_texts2]


extract_soup_udf = F.udf(extract_soup, schema)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == "__main__":
    print("==================")
    files_partitioned = chunks(files, 50)
    for i, file_paths in enumerate(files_partitioned):
        if i > 60:
            try:
                print("==================")
                print(f"We are on partition {i}")
                t = time.time()
                df = spark.read.load(file_paths)
                df2 = df.select("url",extract_soup_udf(F.col("content")).alias("col"))
                df2 = df2.select(F.col("url"),F.col("col.a1").alias("a1"), F.col("col.a2").alias("a2"),F.col("col.wiki_url").alias("wiki_url"), F.col("col.raw_text").alias("raw_text"), F.col("col.hypertexts").alias("hypertext"),F.col("col.text1").alias("text1"), F.col("col.text2").alias("text2")).filter(F.size("col.wiki_url") > 0)
                df2 = df2.withColumn("new", F.arrays_zip("a1","a2","raw_text", "hypertext","wiki_url","text1","text2"))
                df2 = df2.withColumn("new",F.explode("new"))
                df2 = df2.select(F.col("url"),F.col("new.a1").alias("a1"), F.col("new.hypertext").alias("hypertext"),F.col("new.raw_text").alias("raw_text"), F.col("new.a2").alias("a2"), F.col("new.wiki_url").alias("wiki_url"), F.col("new.text1").alias("text1"), F.col("new.text2").alias("text2"))
                df2.write.parquet(f"/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/wiki_content_{i}.parquet", mode = "overwrite")
                print("It took {:.2f} seconds to write one chunk.".format(time.time() - t))
                
            except:
                pass