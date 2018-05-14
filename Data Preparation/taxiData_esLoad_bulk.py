# -*- coding: utf-8 -*-
"""
Created on Sat May 12 2018
@author: Vivek M Agrawal

This module leverages Python Elasticsearch Client to create/update and bulk index
data from PKDD 2015 Prediction Challenge.

Link to source data: www.ecmlpkdd2015.org/ 
Link to Python Elasticsearch Client API documentation: 
	https://elasticsearch-py.readthedocs.io/en/master/
Link to Python Elasticsearch Client Bulk helpers documentation: 
	https://elasticsearch-py.readthedocs.io/en/master/helpers.html

Helper options is used to batch process JSON formatted data from a directory and
load it to local Elasticsearch index. 

TODO:
  * Test if reloading of existing index correctly creates new versions under the
    same id or creates duplicates as index is not explicitly specified
"""

#!/usr/bin/env python

__author__ = "Vivek M Agrawal"
__version__ = "1.0"

ES_INDEX = 'taxi_data'
ES_HOST = {"host": "localhost", "port": 9200}
DATA_DIR = 'jsonData'

import os
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# instantiate Elasticsearch object
es = Elasticsearch(hosts=[ES_HOST])

# check if the index already exists
if es.indices.exists(ES_INDEX):
    # confirm with the user if the existing index can be reused or should be deleted
    # and recreated
    if (input("Press 'Y' to delete and recreate existing index...") == 'Y' or 'y'):
        # delete the existing index based on user input
        res = es.indices.delete(index=ES_INDEX, ignore=[400, 404])
        print("Deleted index {}. ES Response:\n{}".format(ES_INDEX, res))
        # define the index structure to be recreated
        request_body = {
            "settings": {
                "number_of_shards": 2,
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
        # recreated the index
        res = es.indices.create(index=ES_INDEX, body=request_body, ignore=400)
        print("Recreated index {}. ES Response:\n{}".format(ES_INDEX, res))

# indicator for directory with json data to load into the index
jsonDir = os.getcwd() + '//' + DATA_DIR + '//'

# if the directory exists, use bulk api to index the json files contained in
# the directory to index from above step
if os.path.exists(jsonDir):
    # read the content of the directory
    for path, subdirs, files in os.walk(jsonDir):
        # for the different files in the directory
        for fn in files:
            print("Processing file {}...\n".format(fn))
            # open the json file
            with open(os.path.join(jsonDir, fn), 'r') as f:
                # ... and load into a local variable
                rowChunk = json.load(f)
            # load the chunk data to elasticsearch using helper function
            # leverages a generator for creating bulk ingest message by 
            # collating the concatenated document and source data
            bulkIngest = ({
                "_index": ES_INDEX, 
                "_type": "_doc", 
                "_source": chunk}
                    for chunk in rowChunk) # for each chunk
            # at end of operation, load the data to elastic search
            helpers.bulk(es, bulkIngest)
