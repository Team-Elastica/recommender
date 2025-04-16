from sentence_transformers import SentenceTransformer, util
from elasticsearch import Elasticsearch
import pandas as pd
import json
import os

print("Starting...\n")

client = Elasticsearch(
   "https://localhost:9200",
   api_key= "elhUTjhaUUJiM0VoczFmMGl6bmM6b1lLbjljeGNRTEMyc0RRN1A4RmtPdw=="
)

model = SentenceTransformer('all-MiniLM-L6-v2')

# title, genres, overview for Movie
df = pd.read_csv("data/TMDB_movie_dataset_v11.csv")


for start in range(0, len(df), 100):
    chunk = df.iloc[start:start + 100]

    print(f"At {start} / {len(df)} : {(start/len(df)):.2%}")

    bulk_data_list = []
    for i, row in chunk.iterrows():

        try:
            array = model.encode(row["overview"]).tolist()
        except TypeError:
            array = model.encode("").tolist()

        doc = {
            "Title": "EMPTY_TITLE" if pd.isna(row["title"]) else row["title"],
            "Genres": "EMPTY_GENRES" if pd.isna(row["genres"]) else row["genres"],
            "Summary": "EMPTY_SUMMARY" if pd.isna(row["overview"]) else row["overview"],
            "Release Date": "EMPTY_RELEASE_DATE" if pd.isna(row["release_date"]) else row["release_date"],
            "URL": "EMPTY_URL" if pd.isna(row["poster_path"]) else row['poster_path'],
            "Adult": False if pd.isna(row["adult"]) else row["adult"],
            "embedding": array
        }

        bulk_data_list.append(json.dumps({"index": {"_index": "semantic_movie", "_id": str(i)}}))
        bulk_data_list.append(json.dumps(doc))

    bulk_data = "\n".join(bulk_data_list) + "\n"
    
    client.bulk(operations=bulk_data, pipeline="ent-search-generic-ingestion")


print("Finish\n")



