from elasticsearch import Elasticsearch
import json
import os
from tqdm import tqdm

es = Elasticsearch()

json_docs = []
for filename in tqdm(os.listdir("./json")):
    if filename.endswith('.json'):
        with open("./json/" + filename) as open_file:
            #json_docs.append(json.load(open_file))
            es.index(index="twitter",
                    #ignore=400,
                    doc_type='status',
                    body=json.load(open_file))
