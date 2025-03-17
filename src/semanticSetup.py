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
# df = pd.read_csv("data/csv/backlogged_games.csv")

# title, genres, overview for Movie
df = pd.read_csv("data/csv/TMDB_movie_dataset_v11.csv")

for start in range(210000, len(df), 5000):
    chunk = df.iloc[start:start + 5000]

    print(f"At {start} / {len(df)}")

    '''
    #  Game
    bulk_data = ""
    for i, row in chunk.iterrows():

        try:
            array = model.encode(row["Summary"]).tolist()
        except TypeError:
            array = model.encode("").tolist()

        bulk_data += json.dumps({"index": {"_index": "semantic_game", "_id": str(i)}}) + "\n"
        bulk_data += json.dumps({
            "Title": "NONERROR_Title" if pd.isna(row["Title"]) else row["Title"],
            "Genres": "NONERROR_Genres" if pd.isna(row["Genres"]) else row["Genres"],
            "Summary": "NONERROR_Summary" if pd.isna(row["Summary"]) else row["Summary"],
            "embedding": array
        }) + "\n"
    '''

    # Movie
    bulk_data = ""
    for i, row in chunk.iterrows():

        try:
            array = model.encode(row["overview"]).tolist()
        except TypeError:
            array = model.encode("").tolist()

        bulk_data += json.dumps({"index": {"_index": "semantic_movie", "_id": str(i)}}) + "\n"
        bulk_data += json.dumps({
            "Title": "NONERROR_Title" if pd.isna(row["title"]) else row["title"],
            "Genres": "NONERROR_Genres" if pd.isna(row["genres"]) else row["genres"],
            "Summary": "NONERROR_Summary" if pd.isna(row["overview"]) else row["overview"],
            "embedding": array
        }) + "\n"


    client.bulk(operations=bulk_data, pipeline="ent-search-generic-ingestion")

print("Finish\n")



