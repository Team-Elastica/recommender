from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv()

client = Elasticsearch(
  "https://localhost:9200",
   api_key=os.getenv('API')
)

def searchData(name: str, index:str):
    query = {
        "match": {
            "original_title": {
                "query": name,
                "fuzziness": "AUTO"
            }
        }
    }
    
    response = client.search(index=index, query=query)

    return response



# res = searchData('Rush Hour', 'movie')
