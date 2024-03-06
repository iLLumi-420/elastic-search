from elasticsearch import Elasticsearch
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from fastapi import FastAPI, Query
from typing import Optional
from datetime import datetime



disable_warnings(InsecureRequestWarning)
es = Elasticsearch(['https://superadmin:superadmin@localhost:9200'], verify_certs=False)

app = FastAPI()


@app.get("/all_data")
async def get_all_data():
    res = es.search(index='test', body={"query": {"match_all": {}}})
    data = [hit["_source"] for hit in res["hits"]["hits"]]
    return data

def append_function(query, append_term):
    query['bool']['must'].append(append_term)


@app.get("/schools/")
async def get_schools(
    school_code: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
    status: Optional[bool] = Query(None),
    school_type: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    max_latest_year: Optional[int] = Query(None),
    min_latest_year: Optional[int] = Query(None),
    district_code: Optional[str] = Query(None),
    mun_code: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    merged_with: Optional[str] = Query(None),
    head_teacher_name: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    contact_person: Optional[str] = Query(None),
    contact_number: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    min_students: Optional[int] = Query(None),
    max_students: Optional[int] = Query(None),
    estd_date: Optional[str] = Query(None),
    min_estd_date: Optional[str] = Query(None),
    max_estd_date: Optional[str] = Query(None),
):
    query = {
        "bool": {
            "must": []
        }
    }

    if school_code:
        append_function(query, {"term": {"school_code": school_code}})
    if school_name:
        append_function(query, {"match": {"school_name": school_name}})
    if status is not None:
        append_function(query, {"term": {"status": status}})
    if school_type:
        append_function(query, {"term": {"type": school_type}})
    if level:
        append_function(query, {"term": {"level": level}})
    if min_latest_year is not None or max_latest_year is not None:
        append_function(query, {"range": {"latest_year": {"gte": min_latest_year or 0, "lte": max_latest_year or 2081}}})
    if merged_with:
        append_function(query, {"match": {"merged_with": merged_with}})
    if location:
        append_function(query, {"term": {"location": location}})
    if head_teacher_name:
        append_function(query, {"match": {"head_teacher_name": head_teacher_name}})
    if email:
        append_function(query, {"term": {"email": email}})
    if contact_person:
        append_function(query, {"match": {"contact_person": contact_person}})
    if contact_number:
        append_function(query, {"term": {"contact_numbers": contact_number}})
    if year and (min_students is not None or max_students is not None):            
        append_function(query, {"range": {f"students_count.{year}.total": {"gte": min_students or 0, "lte": max_students or 100}}})
    if district_code is not None and mun_code is not None:
        append_function(query, {"wildcard": {"region": f'{district_code}{mun_code}*'}})
    if district_code is not None:
        append_function(query, {"wildcard": {"region": f'{district_code}*'}})
    if estd_date:
        query["bool"]["must"].append({"match": {"estd_date": estd_date}})
    if min_estd_date is not None or max_estd_date is not None:
        append_function(query, {"range": {"estd_year": {"gte": min_estd_date or 0, "lte": max_estd_date or 2081}}})



        
    res = es.search(index='test', body={"query": query})
    data = [hit["_source"] for hit in res["hits"]["hits"]]
    return data

@app.get('/test')
def test():
    query = {
        "query": {
                    "range": {
                        "students_count.2079.total": {
                            "gt": 5
                            }
                        }
                    }
        }
    res = es.search(index='test', body=query)
    data = [hit["_source"] for hit in res["hits"]["hits"]]
    return data



    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, ssl_keyfile="/etc/elasticsearch/key.pem", ssl_certfile="/etc/elasticsearch/cert.pem")