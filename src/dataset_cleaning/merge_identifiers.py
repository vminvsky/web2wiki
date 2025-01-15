
import glob
import os 

import pandas as pd
import numpy as np
import os 
import matplotlib.pyplot as plt

import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyarrow.parquet as pq
import pyarrow as pa

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()

from settings import DATA_DIR, EMBEDDING_DIR

path = "/dlabdata1/wiki_embedding_project/Data/preprocessing/matchings/page_identifers/{}/identifiers.parquet"

def merge_identifiers(langs):
    for lang in langs:
        assert(os.path.exists(path.format(lang)), f"Identifiers do not exist for language {lang}")
    
    identifiers = []
    for lang in langs:
        identifiers.append(f"/dlabdata1/wiki_embedding_project/Data/preprocessing/matchings/page_identifers/{lang}/identifiers.parquet")
    
    all_identifiers = spark.read.load(identifiers[0])
    all_identifiers.createOrReplaceTempView("all_identifiers")

    all_identifiers = spark.sql(f"select *, '{langs[0]}' as wikidb from all_identifiers")

    for iden in identifiers[1:]:
        
        print(iden)
        temp_iden = spark.read.load(iden)
        temp_iden.createOrReplaceTempView("temp_iden")
        temp_iden = spark.sql("select *, '{}' as wikidb from temp_iden".format(iden.split("/")[-2]))

        all_identifiers = all_identifiers.union(temp_iden)
    
    return all_identifiers.toPandas()


    