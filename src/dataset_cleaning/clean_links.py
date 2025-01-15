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

import sys
sys.path.append("/scratch/venia/web2wiki/")

from settings import DATA_DIR

print("====================")

regex_subset = "(\#[\/a-zA-ZäöüÄÖÜßþóúí\_\(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:]+)"
regex_final = "([^#]*)"

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()


@F.udf
def extract_domain(x):
    y = tldextract.extract(x).registered_domain
    return y

@F.udf
def extract_suffix(x):
    y = tldextract.extract(x).suffix
    return y


@F.udf
def wiki_link_processing(x):
    y = x[1:-1]
    return y

@F.udf
def normalise_title(title):
    """ Replace _ with space, remove anchor, capitalize """
    title = title.split("/")[-1]
    title = title.split("#")[0]
    title = urllib.parse.unquote(title)
    title = title.strip()
    if len(title) > 0:
        title = title[0].upper() + title[1:]
    n_title = title.replace("_", " ")
    # if '#' in n_title:
    #     n_title = n_title.split('#')[0]
    return n_title

@F.udf
def extract_language(x):
    if "." in x and len(x) > 8:
        x = x.split(".")[0]
        try:
            x = x.split("://")[1]
        except:
            y = None
        x = x.lower()
    return x


def clean_dataframe():
    df = spark.read.load(DATA_DIR + "all_wikilinks.parquet")
    df = df.withColumn("wiki_links", F.split(wiki_link_processing(F.col("wiki_links")), ", ").alias("wiki_links"))
    df = df.withColumn("domain", extract_domain("url"))
    df = df.withColumn("url_suffix", extract_suffix("url"))
    exploded = df.select("domain", "url", F.explode("wiki_links").alias("wiki_link"))
    exploded = exploded.withColumn('subset', F.regexp_extract('wiki_link', regex_subset, 1))
    exploded = exploded.withColumn('wiki_no_subset', F.regexp_extract('wiki_link', regex_final, 1))
    exploded = exploded.withColumn("wikidb", extract_language("wiki_no_subset"))
    exploded = exploded.withColumn("title", normalise_title("wiki_no_subset"))
    exploded = exploded.withColumn("wikidb", F.regexp_replace("wikidb","/", ""))
    exploded = exploded.filter((F.col("wiki_link").rlike("/wiki/")))
    print("Exporting document")
    exploded.write.parquet(DATA_DIR + "all_wikilinks_clean.parquet", mode = "overwrite")
    

if __name__ == "__main__":
    print("==================")
    clean_dataframe()
