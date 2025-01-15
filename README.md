# The Webonization of Wikipedia

This is the code repository to recreate the analysis from our paper Webonization of Wikipedia: Characterization Wikipedia Linking on the Web''.

The full dataset is available on Zenodo [here](https://zenodo.org/).

### Dataset cleaning
This set of files has some starter code to work with Web2Wiki and process it. In particular, we first extract Wikilinks (extract_wikilinks) from the full dump, then merge them (```merge_wikilinks.py```) and clean them (```clean_links.py```). We then extract language specific sharing through the ```language_shares.py``` file.

We additionally add some code in structural features which processes the original dumps to get structural data surrounding the links. This was used in the Where section of the analysis to understand where within the webpage a link is shared. 

### What
This folder includes a long Jupyter notebook that re-runs the analysis conducted in the paper. The ORES topics can be downloaded [here](https://figshare.com/articles/dataset/Topics_for_each_Wikipedia_Article_across_Languages/12127434).

### Where
For the where analysis we have a few notebooks. First in the ```'where articles invoked.ipynb'``` file we use Homepage2Vec to get Curlie topic distributions over the webpages over a random sample of the Web and the Wikipedia sample. We get the random sample of the Web from domains of the [Common Crawl dump](https://commoncrawl.org/2021/03/february-march-2021-crawl-archive-now-available/). 

We also have the file ```'where and where.ipynb'```. This file explores how different types of websites invoke Wikipedia differently.j

### Why
For the Why analysis, we initially went through a series of steps of iterative coding. This required sampling articles from the Web2Wiki corpus. We have the code used for sampling the articles in the Why folder.

When going through the iterative coding, we often times found the visual cues of the Wikipedia link to be a good indicator for why the link was present. We operationalized this in the screenshots folder (code/screenshots/). This code uses Selenium to highlight the element around a Wikipedia link and screenshots them. 
<p align="center">
<img src="https://i.ibb.co/y6QSdTS/8-clean.png"  width="40%">
</p>

## Additional content
In the visuals, we also include some additional visuals that didn't make it into the final paper that sumamrize some elements of the multilingual aspects of Web2Wiki. 
