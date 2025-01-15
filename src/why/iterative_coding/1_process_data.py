import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.append("/scratch/venia/web2wiki/code/helpers/")

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
from bs4 import BeautifulSoup


from settings import DATA_DIR, EMBEDDING_DIR


import helping_functions as hf
from dotenv import load_dotenv
load_dotenv()


if __name__ == "__main__":
    config = pyspark.SparkConf().setAll([('spark.executor.memory', '80g'),
                                    ('spark.executor.cores', '40'),
                                    ('spark.driver.memory','80g'),
                                    ('spark.driver.maxResultSize','0'),
                                    ('spark.python.worker.memory', '5g'),
                                    ('spark.reducer.maxSizeInFlight','5g'),
                                    ('spark.rpc.message.maxSize', '1000'),
                                    ('spark.sql.autoBroadcastJoinThreshold','-1'),
                                    ('spark.local.dir', '/tmp/')])
    sc = pyspark.SparkContext(conf=config)
    spark = SparkSession.builder.appName('cleaning_dataset').config(conf=config).getOrCreate()
    
    
    web_content = spark.read.load("/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/wiki_content.parquet")
    web_content = hf.clean_web_content(web_content)
        
    crawl_dump =spark.read.option("delimiter", "\t").csv(DATA_DIR + "common_crawl_dump/cc-main-2021-feb-apr-may-domain-ranks.txt.gz", header = True)
    crawl_dump = crawl_dump.withColumn("domain",hf.reverse_reverse_host("#host_rev"))

    en_shares = spark.read.load("/scratch/venia/web2wiki/data/en_shares_merged.parquet")
    en_curlie = spark.read.load("/scratch/venia/web2wiki/data/en_curlie_metadata.parquet")


    # url counts, use this for limiting to sites that only share one url
    url_counts = spark.read.load("/scratch/venia/web2wiki/data/all_url_counts.parquet")
    url_counts = url_counts.withColumnRenamed("count","num_wiki_on_url")


    web_content_sample = hf.sample_files(web_content)
    web_content_sample = web_content_sample.join(url_counts, on = "url")
    web_content_sample = web_content_sample.join(crawl_dump, on ="domain",how="left")
    web_content_sample = web_content_sample.join(en_shares, on = "title", how = "left")
    web_content_sample = web_content_sample.join(en_curlie.select("domain","label","label_leaf"), on = "domain", how = "left")


    web_content_sample = web_content_sample.toPandas()
    web_content_sample["context"] = web_content_sample.apply(lambda x: hf.extract_neighbouring_text(x["text2"], x["wiki_url"]), axis = 1)

    web_content_sample.drop(columns = ["#host_rev", "__index_level_0__"])

    web_content_sample.to_parquet(DATA_DIR + "all_iterative_coding.parquet")
    web_content_sample.sample(1000).to_parquet(DATA_DIR + "sample_iterative_coding.csv", index = False)
