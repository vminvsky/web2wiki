import pandas as pd
import re
import glob

import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import os 
if __name__=="__main__":
    
    os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
    os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
    spark = SparkSession.builder.getOrCreate()

    files = glob.glob("/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/tag_info_55_*")
    df = spark.read.load(files)
    df = df.dropDuplicates(subset = ["url"])
    
    df = df.withColumn("new", F.arrays_zip("wiki_links", "is_tag_footer","is_tag_header","is_tag_aside","is_tag_sup","is_tag_cite","is_tag_p","is_tag_article","is_class_footer","is_class_header","is_class_sidebar","is_class_comment","previous_header","nbhd_text"))\
        .withColumn("new", F.explode("new"))\
        .select("url", F.col("new.wiki_links").alias("wiki_links"), F.col("new.is_tag_footer").alias("is_tag_footer"),F.col("new.is_tag_header").alias("is_tag_header"),F.col("new.is_tag_aside").alias("is_tag_aside"), F.col("new.is_tag_sup").alias("is_tag_sup"),F.col("new.is_tag_cite").alias("is_tag_cite"),F.col("new.is_tag_p").alias("is_tag_p"), F.col("new.is_tag_article").alias("is_tag_article"), F.col("new.is_class_footer").alias("is_class_footer"),F.col("new.is_class_header").alias("is_class_header"),F.col("new.is_class_sidebar").alias("is_class_sidebar"), F.col("new.is_class_comment").alias("is_class_comment"), F.col("new.previous_header").alias("previous_header"), F.col("new.nbhd_text").alias("nbhd_text"))
    df = df.toPandas()

    
    df["is_wiki"] = df["url"].apply(lambda x: 1 if "wiki" in x or "pedia" in x else 0)
    df["url_wiki_count"]=df.groupby("url")["is_tag_footer"].transform("count")

    # df2["is_bibliography"] = df2[]
    
    pattern = re.compile(r"[12][90][89012][0123456789]/[01]\d")
    df["is_blog"] = df["url"].apply(lambda x: 1 if "blog" in x or bool(pattern.search(x)) else 0)
    
    
    df.to_csv("/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/clean_data.csv", index=False)
    