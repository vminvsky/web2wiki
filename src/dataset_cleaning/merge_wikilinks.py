import pandas as pd
from settings import RAW_PARQUETS

import glob 
files = glob.glob(RAW_PARQUETS.format("*"))

df = pd.DataFrame()
for file in files:
    print(file)
    df = df.append(pd.read_parquet(file))
    
df.to_parquet("/scratch/venia/web2wiki/data/all_wikilinks.parquet")