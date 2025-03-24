import pandas as pd
import json
import os

from elasticsearch import Elasticsearch

print("Starting...\n")

client = Elasticsearch(
  "https://localhost:9200",
  api_key=os.getenv("API")
)

# Load TV
# df = pd.read_csv("data/TMDB_tv_dataset_v3.csv")

# Movie
# df = pd.read_csv("data/TMDB_movie_dataset_v11.csv")

# Game
df = pd.read_csv("data/csv/backlogged_games.csv")

f = open("data/game.json", "w")

for start in range(0, len(df), 5000):
    chunk = df.iloc[start:start + 5000]

    # Convert to Elasticsearch bulk JSON format
    bulk_data = ""
    for i, row in chunk.iterrows():
        bulk_data += json.dumps({"index": {"_index": "game", "_id": str(i)}}) + "\n"
        bulk_data += row.to_json() + "\n"

    # client.bulk(operations=bulk_data, pipeline="ent-search-generic-ingestion")
    f.write(bulk_data)

f.close()

print("Finish\n")
