from elasticsearch import Elasticsearch
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
import numpy as np
import pandas as pd


disable_warnings(InsecureRequestWarning)

es = Elasticsearch(['http://superadmin:superadmin@localhost:9200'], verify_certs=True)


def list_indices():

    res = es.cat.indices(format="json")

    index_names = [index['index'] for index in res]

    return index_names

# indices = list_indices()
# for index in indices:
#     print(index)
#     es.indices.delete(index=index)


# id1_doc1 = {
#     'author': 'Sandeep Parajuli',
#     'text': 'Elasticsearch'
# }
# id1_doc2 = {
#     'author2': "2",
#     'text': 'Elasticsearch'
# }

# id2_doc1 = {
#     'test' : 'new index',
#     'text': 'Elasticsearch'
# }

# es.index(index='test-index1', id=1, body=id1_doc1, refresh=True)
# es.index(index='test-index1', id=2, body=id1_doc2, refresh=True)
# es.index(index='test-index2', id=1, body=id2_doc1, refresh=True)






def count_documents(index_name):

    count_query = {
        "query": {
            "match_all": {}
        }
    }

    res = es.count(index=index_name, body=count_query)

    document_count = res['count']

    print(f"Number of documents in index '{index_name}': {document_count}")



def get_index_contents(index):
    search_query = {
        'query':{
            "match_all": {}
        }
    }
    res = es.search(index=index, body=search_query)

    data = [{each['_id']: each['_source']} for each in res['hits']['hits']]

    print(data)


def search_term(index, search_term):
    search_query = {
        "query": {
            "match": {
                "text": search_term
            }
        }
    }

    res = es.search(index=index, body=search_query )
    sources = [hit['_source'] for hit in res['hits']['hits']]

    print(sources)


def delete_by_id(index_name, doc_id):

    print('before deleting')
    get_index_contents(index_name)

    res = es.delete(index=index_name, id=doc_id, refresh=True)

    print('after deleting')

    get_index_contents(index_name)



def match_query(index_name, field_name, query_text):

    match_query = {
        "query": {
            "match": {
                field_name: query_text
            }
        }
    }

    res = es.search(index=index_name, body=match_query)

    for hit in res['hits']['hits']:
        print(hit['_source'])

def fuzzy_search(index_name, field_name, query, max_expansions):

    body = {
        "query": {
            "fuzzy": {
                field_name: {
                    "value": query,
                    "fuzziness": "AUTO",
                    "max_expansions": max_expansions
                }
            }
        }
    }

    results = es.search(index=index_name, body=body)

    if 'hits' in results and 'hits' in results['hits']:
        return [{hit['_id']: hit['_source']} for hit in results['hits']['hits']]
    
    return []
    


def match_results(filed_name, value):

    query = {
        "query": {
            "match": {
                filed_name: value
            }
        }
    }
    
    
    res = es.search(index='school_index', body=query)

    return [{each['_id']: each['_source']} for each in res['hits']['hits']]

def perform_bool_search(index_name):

    bool_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"level": "Primary"}},
                    {"match": {"type": "Public"}}
                ]
            }
        }
    }

    try:

        res = es.search(index=index_name, body=bool_query)
        return [{each['_id']: each['_source']} for each in res['hits']['hits']]
    except Exception as e:
        print(f"An error occurred during the search: {e}")
        return None

def perform_range_search(index_name, field, min_value):
  
    try:
        # Define the range query
        range_query = {
            "query": {
                "range": {
                    field: {
                        "gt": min_value
                    }
                }
            }
        }

        res = es.search(index=index_name, body=range_query)
        return [{each['_id']: each['_source']} for each in res['hits']['hits']]
    except Exception as e:
        print(f"An error occurred during the search: {e}")
        return None
    
def extract_region(loc_id):
    loc_id_str = str(loc_id)
    province_code = loc_id_str[:1]
    district_code = loc_id_str[:3]
    municipality_code = loc_id_str[3:5]
    ward_number = loc_id_str[5:]
    return {
        "province_code": province_code,
        "district_code": district_code,
        "municipality_code": municipality_code,
        "ward_number": ward_number
    }

def clean_contact_numbers(contact_numbers):
    if contact_numbers is None:
        return None
    else:
        numbers = [num.strip() for num in contact_numbers.split(',') if num.strip().isdigit()]
        return numbers if numbers else None
    
def index_csv_data_to_es(index_name):

    school_path = "../complete_data/school_data.csv"
    location_path = "../complete_data/location_data.csv"
    students_count = "../complete_data/students_number.csv"

    school_data = pd.read_csv(school_path)
    location_data = pd.read_csv(location_path)
    students_data = pd.read_csv(students_count)

    school_data = school_data.iloc[200:350]
    # location_data = location_data.iloc[:100]
    # students_data = students_data.iloc[:100]

    location_data.replace(np.nan, None, inplace=True)    
    students_data.replace(np.nan, None, inplace=True)   
    school_data.replace(np.nan, None, inplace=True)    


    school_data['status'] = school_data['status'].apply(lambda x: True if x==1 else False)
    school_data['contact_numbers'] = school_data['contact_numbers'].apply(clean_contact_numbers)

    school_data.rename(columns={
        'loc_id' : 'region'
    }, inplace=True)

    students_count_grouped = students_data.groupby('school_code').apply(lambda x:
        {
            f'{row["year"]}': {
            'year': row['year'],
            'year_ad': row['year_ad'],
            'ecd': row['ecd_total'],
            '1-5': row['g1_5_total'],
            '6-8': row['g6_8_total'],
            '9-10': row['g9_10_total'],
            '11-12': row['g11_12_total'],
            'total': row['total']
        } for i, (_, row) in enumerate(x[['year', 'year_ad','ecd_total', 'g1_5_total', 'g6_8_total', 'g9_10_total', 'g11_12_total', 'total']].iterrows())
        }).reset_index(name='students_count')

    school_data = pd.merge(school_data, students_count_grouped, on='school_code', how='left')

    school_data.replace(np.nan, None, inplace=True)    



    
    mappings = {
        "mappings": {
            "properties": {
                "school_code": {"type": "keyword"},
                "school_name": {"type": "text"},
                "status": {"type": "boolean"},
                "type": {"type": "keyword"},
                "level": {"type": "keyword"},
                "latest_year": {"type": "integer"},
                "latest_year_ad": {"type": "integer"},
                "location": {"type": "keyword"},
                "estd_date": {
                    "type": "date",
                    "format": "yyyy-MM-dd" 
                },
                "estd_date_ad": {
                    "type": "date",
                    "format": "yyyy-MM-dd" 
                },
                "merged_with": {"type": "text"},
                "head_teacher_name": {"type": "text"},
                "email": {"type": "keyword"},
                "contact_person": {"type": "text"},
                "contact_numbers": {"type": "keyword"},
                "region": {"type": "text"},
                "students_count": {
                    "properties": {
                        "year": {"type": "integer"},
                        "year_ad": {"type": "integer"},
                        "ecd": {"type": "integer"},
                        "1-5": {"type": "integer"},
                        "6-8": {"type": "integer"},
                        "9-10": {"type": "integer"},
                        "11-12": {"type": "integer"},
                        "total": {"type": "integer"}
                    }
                }
            }
        }
    }

    es.indices.create(index='test', body=mappings)

    

    for idx, row in school_data.iterrows():
        es.index(index=index_name, id=idx, body=row.to_dict())


if __name__ == '__main__':


    # indices = list_indices()

    # index_name = "school_index"
    index_csv_data_to_es('test')


    # get_index_contents(index_name)

    # count_documents(index_name)

    # delete_by_id(index_name, 0)

    # get_index_contents(index_name)

    # print(match_results('level', 'Primary'))

    # print(perform_bool_search(index_name))

    # print(perform_range_search(index_name, "latest_year_ad", 2021))


    
    




