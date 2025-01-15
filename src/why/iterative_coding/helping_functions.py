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

# ensure english
from langdetect import detect
from whatthelang import WhatTheLang

@F.udf
def extract_subdomain(x):
    if len(tldextract.extract(x).subdomain) > 1:
        y = tldextract.extract(x).subdomain +"."+tldextract.extract(x).registered_domain
    else:
        y = tldextract.extract(x).registered_domain
    return y

@F.udf
def extract_domain(x):
    y = tldextract.extract(x).registered_domain
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


def clean_web_content(web_content,drop_dups = True):
    """
    Run this once... To generate the metadata file.
    """
    web_content = web_content.dropna(subset = ["url","wiki_url"])
    if drop_dups == True:
        web_content.dropDuplicates(subset=["url","wiki_url"])
    web_content=web_content.withColumn("title", normalise_title("wiki_url"))
    web_content = web_content.withColumn("subdomain", extract_subdomain("url"))
    web_content = web_content.withColumn("domain", extract_domain("url"))
    web_content = web_content.filter(F.col("title") != "")
    return web_content


# this fn is necessary for reversing the host names
@F.udf 
def reverse_reverse_host(x):
    x = x.split(".")
    x = x[-1::-1]
    x = ".".join(x)
    return x


@F.udf
def detect_lang(x):
    wtl = WhatTheLang()
    try: 
        return wtl.predict_lang(x)
    except:
        return None
@F.udf
def extract_wiki_text(row):
    bs = BeautifulSoup(row,"xml")
    text = bs.get_text()
    if len(text)< 2:
        text = "None"
    return text
    
def sample_files(web_content, with_lang = True, prop = 1.0):
    web_content_sample = web_content.sample(prop)
    if with_lang == True:
        web_content_sample = web_content_sample.withColumn("lang", detect_lang("text1"))
        web_content_sample = web_content_sample.filter(F.col("lang") == "en")
    web_content_sample = web_content_sample.withColumn("text", extract_wiki_text(F.col("text1")))
    return web_content_sample


def extract_neighbouring_text(x,text, num_chars = 150):
        """
        Extracts the neighbouring text of the Wikipedia mention
        """
        ind = x.find(text)
        n = len(x)
        m = len(text)

        if ind - num_chars < 0:
            ind_0 = 0
        else: ind_0 = ind - num_chars
        if ind + num_chars > n:
            return x[ind_0:ind] + " [BREAK] " +x[ind:ind+m] + " [BREAK] " + x[ind + m:]
        else: 
            ind_1 = ind + num_chars + 50
            return x[ind_0:ind] + " [BREAK] " +x[ind:ind+m] + " [BREAK] " + x[ind + m: ind_1]



def extract_comments(x):
    try:
        bs = BeautifulSoup(x, "xml")
        tags = bs.findall()
        tags_return = []
        for tag in tags:
            if tag.has_attr(re.compile("comment")):
                return True
            else:
                return False
    except: 
        return None