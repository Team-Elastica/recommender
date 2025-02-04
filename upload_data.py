import pandas as pd
import json
import requests

# Load CSV
df = pd.read_csv("../ElasticaData/TMDB_tv_dataset_v3.csv", skiprows=[835974])

for start in range(0, len(df), 5000):
    chunk = df.iloc[start:start + 5000]

    # Convert to Elasticsearch bulk JSON format
    bulk_data = ""
    for i, row in chunk.iterrows():
        bulk_data += json.dumps({"index": {"_index": "tvshows_index", "_id": str(i)}}) + "\n"
        bulk_data += row.to_json() + "\n"

    # Send to Elasticsearch
    response = requests.post("http://localhost:9200/tvshows_index/_bulk",
                            headers={"Content-Type": "application/json"},
                            data=bulk_data)
    
    if response.status_code != 200:
        print("⚠️ Error in bulk upload:", response.text)
        break  # Stop if there's an error

print(response.text)
print(response.json())
