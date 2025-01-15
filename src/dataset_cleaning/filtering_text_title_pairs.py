import pandas as pd
import torch
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import cosine

DATA_PATH = "/scratch/venia/web2wiki/data/web_content/"

class TextualSimilarity:
    def __init__(self, similarity_column, model_name='sentence-transformers/paraphrase-albert-small-v2'):
        self.model = SentenceTransformer(model_name)

    def similarity_score(self, sentence1, sentence2):
        vector1 = self.model.encode([sentence1])[0]
        vector2 = self.model.encode([sentence2])[0]
        return 1 - cosine(vector1, vector2)

    def calculate_similarity(self, df, col1, col2, batch_size=128):
        for i in range(0, df.shape[0], batch_size):
            batch = df[i:i+batch_size]
            batch['similarity'] = batch.apply(lambda row: self.similarity_score(row[col1], row[col2]), axis=1)
        return df

def main():
    for i in range(20):
        ts = TextualSimilarity(similarity_column)
        df = pd.read_parquet(f"/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/wiki_content_{i}.parquet")
        df = ts.calculate_similarity(df, 'col1', 'col2')
        print(df)

if __name__ == "__main__":
    main()