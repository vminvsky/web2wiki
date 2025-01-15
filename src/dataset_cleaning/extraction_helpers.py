import numpy as np
from bs4 import BeautifulSoup
import re

import pyspark.sql.functions as F
from pyspark.sql.types import *

regex_search = r"(en.wikipedia\.org[\s\/a-zA-ZäöüÄÖÜßþóúí\_\?(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:\!]+)"


def find_neighbouring_links(all_links):
    all_links2 = []
    for link in all_links:
        rand_num = np.random.rand()
        if rand_num > 0.5:
            b = link.find_next(re.compile('^a$'))
        else:
            b = link.find_previous(re.compile('^a$'))
        if b:
            all_links2.append(b)
        else:
            pass
    return all_links2
        

def extract_neighbouring_text2(x,text, num_chars = 150):
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


# extract_neighbouring_text = F.udf(extract_neighbouring_text, ArrayType(StringType()))

def nbhd_text(x):
    hypertext = x.get_text()
    text = x.parent.parent.get_text()
    return extract_neighbouring_text2(text, hypertext)
    

def process_headers(x):
    soup = BeautifulSoup(x,features="lxml")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
#     all_links2 = find_neighbouring_links(all_links)
    all_links = all_links 

    resp = []
    for elt in all_links:
        resp.append(extract_header(elt))
    return resp

def is_tag(elt,tag,val = None):
    if val == None:
        val = 0
    if elt.name == tag:
        val += 1
    elif (elt.name != "html"):
        val += is_tag(elt.parent, tag,val)
    return val



def extract_header(elt):
    prev_header = elt.find_previous(re.compile('^h[1-6]$'))
    if (prev_header != None):
        if prev_header.parent == elt.parent.parent.parent:
            prev_name = prev_header.text
        else:
            prev_name = "None"
    else:
        prev_name = "None"
        distance = "None"
    return prev_name

# process_headers = F.udf(process_headers, ArrayType(StringType()))

def is_class(elt, key = None, val=None):
#     elt = elt.parent
    keys = class_variables.all_classes[key]
    if val == None:
        val = 0
    pattern = re.compile("|".join(keys))
    if elt.has_attr("class"):
        try:
            if len(pattern.findall(elt["class"][0])) > 0:
                val += 1
            else:
                val += 0
        except:
            val += 0
    if elt.has_attr("id"):
        try: 
            if len(pattern.findall(elt["id"][0])) > 0:
                val += 1
            else:
                val += 0
        except:
            val += 0
    elif (elt.name != "html"):
        val += is_class(elt.parent, key, val)
    return val

def process_all(x: str):
    soup = BeautifulSoup(x,features="lxml")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
    classes = []
    headers = []
    tags = []
    nbhd_texts = []
    hrefs = []
    for elt in all_links:
        hrefs.append(elt.get("href"))
        classes.append(process_class(elt))
        tags.append(process_tags(elt))
        nbhd_texts.append(nbhd_text(elt))
        headers.append(extract_header(elt))
    final_list = []
    for link, header, c, t, text in zip(hrefs,headers,classes,tags,nbhd_texts):
        final_list.append([link,header,t,c,text])
    return final_list
process_all_udf = F.udf(
    process_all, ArrayType(
        StructType(
            [StructField("href", StringType()),
             StructField("header", StringType()),
             StructField("tags", ArrayType(StructType([
                 StructField("tag", StringType()),
                 StructField("tag_count",IntegerType())
             ]))),
                StructField("classes", ArrayType(StructType([
                 StructField("class", StringType()),
                 StructField("class_count",IntegerType())
             ]))),
             StructField("nbhd_text",StringType())
            ])))


def process_tags(elt):
    tags = tag_instance_of()
    for tag in tags.tags():
        tags[tag] = tags[tag]+is_tag(elt, tag)
    return tags._list()
# process_tags = F.udf(process_tags, ArrayType(ArrayType(StructType([StructField("tag", StringType()),StructField("count", IntegerType())]))))

    
def process_class(elt):
    class_vals = class_instance_of()
    for c in class_variables.all_classes.keys():
        class_vals[c] = class_vals[c]+is_class(elt, c)
    return class_vals._list()


# process_class = F.udf(process_class, ArrayType(ArrayType(StructType([StructField("class", StringType()),StructField("count", IntegerType())]))))
    
def extract_soup(x):
    soup = BeautifulSoup(x,features="lxml")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
#     all_links2 = find_neighbouring_links(all_links)

    k = [k.get("href") for k in all_links]
    return k
# extract_soup = F.udf(extract_soup, ArrayType(StringType()))



class tag_instance_of:
#     footer: int = 0
#     header: int = 0
#     sup: int = 0
        
    def __init__(self):
        self.footer = 0
        self.header = 0
        self.sup = 0
    
    def tags(self):
        return ["footer","header","sup"]
        
    def __getitem__(self,item):
        return self.__dict__[item]

    def __setitem__(self,item,value):
        self.__dict__[item] = value
        
    def _list(self):
        return [("footer", self.footer), ("header", self.header), ("sup", self.sup)]
    
class class_variables:
    class_footer = ["footer", "bottomMenu"]
    class_header = ["header", "topmenu","topMenu"]
    class_sidebar = ["sidebar","rightside","widget","module","right-hand-side"\
        ,"left-hand-side","leftside", "blogroll", "banner","righter","left-side","right-side","info-bar", "panel"]
    class_second_order = ["msg","comment","message"]
    
    all_classes = {"class_footer":class_footer, "class_header":class_header, "class_sidebar":class_sidebar, "class_second_order":class_second_order}
    

class class_instance_of:
    def __init__(self):
        self.class_footer: int = 0
        self.class_header: int = 0
        self.class_sidebar: int = 0
        self.class_second_order: int = 0
    
    def __getitem__(self,item):
        return self.__dict__[item]

    def __setitem__(self,item,value):
        self.__dict__[item] = value
    
    def classes(self):
        return ["class_footer","class_header","class_sidebar","class_second_order"]
        
    def _list(self):
        return [("footer", self.class_footer), ("header", self.class_header), ("sidebar", self.class_sidebar), ("response",self.class_second_order)]


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]