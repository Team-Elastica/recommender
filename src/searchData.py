from elasticsearch import Elasticsearch

client = Elasticsearch(
  "https://localhost:9200",
   api_key= "API KEY HERE"
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
