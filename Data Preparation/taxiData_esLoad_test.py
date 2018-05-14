# -*- coding: utf-8 -*-
"""
Created on Sat May 12 2018
@author: Vivek M Agrawal

This module leverages Python Elasticsearch Client to create and index taxi
data from PKDD 2015 Prediction Challenge.

Link to source data: www.ecmlpkdd2015.org/ 
Link to API documentation: https://elasticsearch-py.readthedocs.io/en/master/

Both the bulk API and helper options are explored in the code. Reader can further
evaluate efficiencies of either of the approaches with loaded volumes. 
"""

#!/usr/bin/env python

__author__ = "Vivek M Agrawal"
__version__ = "1.0"

ES_INDEX = 'taxi_data'
ES_HOST = {"host": "localhost", "port": 9200}

import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# instantiate Elasticsearch object
es = Elasticsearch(hosts=[ES_HOST])

#check if the index already exists
if es.indices.exists(ES_INDEX):
    # if exists, delete the existing index
    res = es.indices.delete(index=ES_INDEX, ignore=[400, 404])
    print("ES Response: {}".format(res))

# define the index structure
request_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": { 
        "_doc": { 
            "properties": {
                "call_type": {"type": "text"},
                "customer_id": {"type": "text"},
                "partial_location_flag": {"type": "boolean"},
                "surge_rate": { "type": "text"},
                "taxi_id": {"type": "text"},
                "taxi_stand_id": {"type": "text"},
                "trip_id": {"type": "text"},
                "trip_instance_id": {"type": "text"},
                "trip_instance_location": {"type": "geo_point"},
                "trip_start_time": {"type": "date",
                    "format": "strict_date_optional_time||epoch_millis"}
            }
        }
    }
}

# ignore 400 cause by IndexAlreadyExistsException when creating an index
res = es.indices.create(index=ES_INDEX, body=request_body, ignore=400)
print("ES Response: {}".format(res))

with open(".//jsonData//rowChunk_0.json", 'r') as f:
    rowChunk = json.load(f)

## without helper function
## initialize the document data to be applied to all records
#doc = [{
#        "index":{
#            "_index": ES_INDEX, 
#            "_type": "_doc"}
#        }]
## create a placeholder to collate concatenated document and source data
#bulkIngest = str()
## for all the json chunks in the source data
#for chunk in rowChunk:
#    # append the chunk to the bulk ingest payload
#    bulkIngest = bulkIngest + \
#    str(json.dumps(doc[0]) + '\n' + json.dumps(chunk) + '\n')
## at end of operation, load the data to elastic search
#es.bulk(body=bulkIngest)

# using helper function
# leverages a generator for creating bulk ingest message by collating the
# concatenated document and source data
bulkIngest = ({
    "_index": ES_INDEX, 
    "_type": "_doc", 
    "_source": chunk}
        for chunk in rowChunk) # for each chunk
# at end of operation, load the data to elastic search
helpers.bulk(es, bulkIngest)
