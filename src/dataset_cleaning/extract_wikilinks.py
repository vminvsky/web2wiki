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

import glob 
files = glob.glob(WIKI_PAGES_DIR + "/*")
# regex_search = "([\:\/a-zA-Z\.]+wikipedia\.org[\/a-zA-ZäöüÄÖÜßþóúí\_\(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:]+)"
regex_search = r"(en.wikipedia\.org[\s\/a-zA-ZäöüÄÖÜßþóúí\_\?(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:\!]+)"


@F.udf
def extract_regex(x):
    links = re.findall(regex_search, x)
    return links

@F.udf
def extract_soup(x):
    soup = BeautifulSoup(x,features="lxml")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
    k = [k.get("href") for k in all_links]
    return k


def run(data):
    data = data.withColumn("wiki_links", extract_soup(F.col("content")))
    selected = data.select("url", "wiki_links")
    return selected

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == "__main__":
    print("==================")
    files_partitioned = chunks(files, 400)
    for i, file_paths in enumerate(files_partitioned):
        try:
            print("==================")
            print(f"We are on partition {i}")
            t = time.time()
            data = spark.read.load(file_paths)
            selected = run(data)
            selected.write.parquet(f"/scratch/venia/web2wiki/data/wikilinks_extracted_soup/wiki_links_{i}.parquet")
            print("It took {:.2f} seconds to write one chunk.".format(time.time() - t))
        except:
            print(i)

