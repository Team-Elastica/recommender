from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
import os

model = SentenceTransformer('all-MiniLM-L6-v2')

client = Elasticsearch(
  "https://localhost:9200",
   api_key= "API KEY HERE"
)

inputQuery = "Death Stranding 2"

# https://medium.com/@bairagiabhishek03/elasticsearch-as-a-vector-store-es-tutorial-5-816f9451ddc1
query = {
    "script_score": {
        "query": {"match_all": {}},
        "script": {
            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
            "params": {
                "query_vector": model.encode(inputQuery).tolist()
            }
        }
    }
}

response = client.search(size=5, source_excludes='embedding' ,index='semantic_game', query=query)

for hit in response['hits']['hits']:
    print(hit['_source']['Title'])