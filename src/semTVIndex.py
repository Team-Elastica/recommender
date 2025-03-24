from sentence_transformers import SentenceTransformer, util
from elasticsearch import Elasticsearch
import pandas as pd
import json
import os

print("Starting...\n")

client = Elasticsearch(
  "https://localhost:9200",
   api_key= "API KEY HERE"
)

model = SentenceTransformer('all-MiniLM-L6-v2')

# name, genres, overview for TV
df = pd.read_csv("data/csv/TMDB_tv_dataset_v3.csv")

for start in range(0, len(df), 5000):
    chunk = df.iloc[start:start + 5000]

    print(f"At {start} / {len(df)} : {(start/len(df) * 100):.2%}")

    bulk_data_list = []

    for i, row in chunk.iterrows():
        try:
            array = model.encode(row["overview"]).tolist()
        except TypeError:
            array = model.encode("").tolist()
        
        doc = {
            "Title": "EMPTY_TITLE" if pd.isna(row["name"]) else row["name"],
            "Genres": "EMPTY_GENRES" if pd.isna(row["genres"]) else row["genres"],
            "Summary": "EMPTY_SUMMARY" if pd.isna(row["overview"]) else row["overview"],
            "Release Date": "EMPTY_RELEASE_DATE" if pd.isna(row["first_air_date"]) else row["first_air_date"],
            "URL": "EMPTY_URL" if pd.isna(row["backdrop_path"]) else row["backdrop_path"],
            "embedding": array
        }

        bulk_data_list.append(json.dumps({"index": {"_index": "semantic_tv", "_id": str(i)}}))
        bulk_data_list.append(json.dumps(doc))

    bulk_data = "\n".join(bulk_data_list) + "\n"

    client.bulk(operations=bulk_data, pipeline="ent-search-generic-ingestion")
    

print("Finish\n")



