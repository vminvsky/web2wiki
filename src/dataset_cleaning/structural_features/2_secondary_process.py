import pandas as pd
from scipy.stats import entropy
import tldextract
import urllib
import re 
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
import os

import matplotlib.pyplot as plt
sys.setrecursionlimit(10000)

import re
import time
from bs4 import BeautifulSoup
sys.path.append("/scratch/venia/web2wiki/code/helpers/")




def try_float(x):
    try:
        float(x)
        return True
    except:
        return False


def extract_subdomain(x):
    if len(tldextract.extract(x).subdomain) > 1:
        y = tldextract.extract(x).subdomain +"."+tldextract.extract(x).registered_domain
    else:
        y = tldextract.extract(x).registered_domain
    return y


def is_wiki(x):
    if ("wiki" in x) or ("pedia" in x):
        return 1
    else: 
        return 0 
    

pattern = re.compile(r"[12][90][89012][0123456789]/[01]\d")

def is_blog(x):
    if ("blog" in x) or (bool(pattern.search(x))): 
        return 1
    else:
        return 0


def header(x):
    if ("reference" in x.lower()) or ("bibliography" in x.lower()) or ("sources" in x.lower()):
        return 1 
    else:
        return 0 
header_udf = F.udf(header, IntegerType())
    
def attribution(x):
    if "File:" in str(x) or "Image:" in str(x):
        return 1 
    else:
        return 0 
    
attribution_udf = F.udf(attribution, IntegerType())

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

if __name__=="__main__":
    df = pd.read_parquet("/scratch/venia/web2wiki/data/test/cleaner_data.parquet")
    order_0 = ["tag_footer", "tag_header","class_footer","class_header","class_sidebar"]

    evidence = ["tag_sup","header_reference"]
    order_2 = ["class_response"]

    df["0th_order"] = df[order_0].astype(float).sum(axis = 1)

    df["2nd_order"] = df[order_2].astype(float).sum(axis = 1)
    print("Checkpoint: 4")

    df.loc[df["2nd_order"]>0,"order"] = 2
    df.loc[df["0th_order"]>0,"order"] = 0
    df["order"] = df["order"].fillna(1)
    
    df["domain"] = df["url"].astype(str).apply(lambda x: extract_subdomain(x))
    df["domain_count"] = df.groupby("domain")["url"].transform("count")

    print("Writing file")
    df.to_csv("/scratch/venia/web2wiki/data/validation/across_orders.csv",index=False)
