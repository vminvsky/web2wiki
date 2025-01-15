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

class sample_articles():
    """
    This class is used to call various sampling techniques for 
    sampling the articles for the iterative coding. 
    """
    def __init__(self, web_contentt):
        """
        web_contentt: this is the cleaned web2wiki content. 
        """
        self.web_content = web_contentt.copy()
        self.sampled = None
        self.sampled_html = None
        
    def map_html_structure(self, word_changes):
        web_content = self.web_content
        web_content.loc[web_content["a1"].isin(word_changes), "a1"] = web_content.loc[web_content["a1"].isin(word_changes), "a2"]
        web_content.loc[web_content["a1"].str.match("h[1-6]"), "a1"] = "header"
        return web_content


    def article_sample(self, sample_col = "title", buckets = 5, sample_number = 15):
        """
        This file samples articles based on buckets.
        We drop duplicates on domains to get many different domains that invoke articles
        instead of the dominant several. 
        
        sample_col: this will be the column that we use to define buckets over
        buckets: determines the number of buckets we use to sample over
        sample_number: how many samples we want from each bucket. 
        """
        assert self.sampled == None, "Already sampled"
        web_content = self.web_content
        web_content["article_count"] = web_content.groupby([sample_col])[sample_col].transform("count")
        web_content["bucket"] = pd.cut(web_content["article_count"].rank(pct=True), buckets, labels=False)
        web_content = web_content.drop_duplicates(subset = ["domain"])
        sampled = pd.DataFrame()
        for i in range(buckets):
            sampled = sampled.append(web_content[web_content["bucket"] == i].sample(sample_number))
    
        self.sampled = sampled
        return sampled
    
    def sample_by_num_links_on_url(self,nums):
        num1 = nums[0]
        start = self.web_content[self.web_content["count"] == 1].sample(num1)
        for i, k in enumerate(nums[1:],2):
            temp = self.web_content[self.web_content["count"] == i].sample(k)
            start = start.append(temp)
        self.sampled_links = start
        return start
        
    def sample_html_structure(self, tags,sample_number = 7, word_changes = None):
        """
        Sample over the HTML structure.
        For each of the in tags
        
        tags: a list of HTML tags we wish to restrict to        
        sample_number: the number of rows we wish to sample
        word_changes: sometimes, a1 may be a bold or some other feature, in this case we want to look up one level.
        """
        assert self.sampled_html == None, f"Already sampled"
        if word_changes != None:
            web_content = self.map_html_structure(word_changes)
        else:
            web_content = self.web_content
        
        web_content = web_content[web_content["a1"].isin(tags)]
        tag_count = web_content.groupby("a1")["title"].count().reset_index()
        tag_count = tag_count[tag_count["title"] > 9]
        web_content = web_content.drop_duplicates(subset = ["domain"])
        web_content= web_content[web_content["a1"].isin(tag_count.a1)]
        sampled = web_content.groupby("a1").sample(sample_number, replace = True)
        sampled =  sampled.drop_duplicates()
        
        self.sampled_html= sampled

        return sampled, tag_count
    
    def list_append(self, subset_articles, sample_number = 5):
        """
        We sample one article many times to see the different contexts it can arise in.
        
        subset_articles: a list of articles that we want to sample over.
        """
        web_content = self.web_content
        web_content = web_content[web_content["title"].isin(subset_articles)]
        web_content = web_content.drop_duplicates(subset = "domain")
        appended_vals = web_content.groupby("title").sample(sample_number)
        return appended_vals


def process_sample(sample, harmonic = False):
    """
    This will process a sample to make it clean for iterative coding
    
    sample: this is the sample of shares we want to process
    harmonic: if we include harminoc position
    """   
    if harmonic == True:
        sample = sample[["url","num_wiki_on_url", "domain","subdomain","#harmonicc_pos","a1", "a2", "title","context","text","total_count","total_views", "topic_leaf"]]
    else: 
        sample = sample[["url", "num_wiki_on_url","domain","subdomain","a1", "a2", "title","context","text","total_count","total_views", "topic_leaf"]]
    sample = sample.rename({"a1":"parent_1", "a2":"parent_2"},axis = 1)
    return sample




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
    
    