from sentence_transformers import SentenceTransformer, util
from elasticsearch import Elasticsearch
import pandas as pd
import json
import os

print("Starting...\n")

client = Elasticsearch(
  "https://localhost:9200",
  api_key=os.getenv("API")
)

model = SentenceTransformer('all-MiniLM-L6-v2')

# Title, Genre, Summary for Game
df = pd.read_csv("data/csv/backlogged_games.csv")


for start in range(0, len(df), 5000):
    chunk = df.iloc[start:start + 5000]

    print(f"At {start} / {len(df)}")

    #  Game
    bulk_data_list = []
    for i, row in chunk.iterrows():

        try:
            array = model.encode(row["Summary"]).tolist()
        except TypeError:
            array = model.encode("").tolist()
        
        doc = {
            "Title": "EMPTY_TITLE" if pd.isna(row["Title"]) else row["Title"],
            "Genres": "EMPTY_GENRES" if pd.isna(row["Genres"]) else row["Genres"],
            "Summary": "EMPTY_SUMMARY" if pd.isna(row["Summary"]) else row["Summary"],
            "Release Date": "EMPTY_RELEASE_DATE" if pd.isna(row["Release_Date"]) else row["Release_Date"],
            "URL": "EMPTY_URL",
            "embedding": array
        }

        bulk_data_list.append(json.dumps({"index": {"_index": "semantic_game", "_id": str(i)}}))
        bulk_data_list.append(json.dumps(doc))

        bulk_data = "\n".join(bulk_data_list) + "\n"

        try:
            client.bulk(operations=bulk_data, pipeline="ent-search-generic-ingestion")
        except:
            file = open("error.txt", 'w')
            file.write(bulk_data)
            file.close()

print("Finish\n")



