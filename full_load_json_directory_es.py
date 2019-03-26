#!/usr/bin/env python
from elasticsearch import Elasticsearch
import json
import os
from tqdm import tqdm
import click
from art import tprint

@click.command()
@click.option('-f','--folder', help='Folder with json files to load into ElasticSearch (default ./json/) ', type=str , default="./json/")
@click.option('-e','--elasticuri', help='Elastic search uri f.e. http://127.0.0.1:9200 (default)', type=str , default="http://127.0.0.1:9200/")
@click.option('-x','--elasticindex', help='Elastic search Index (default twitter)', type=str , default="twitter")
def load_json_es(folder: str = "./json/", elasticuri: str = "http://127.0.0.1:9200/" , elasticindex:str="twitter"):
    """Reads all json files in a folder an puts in a Elasticsearch index
    
    Keyword Arguments:
        folder {str} -- Folder that contains jsons (default: {"./json/"})
        elasticuri {str} -- Uri to ElasticSearch (default: {"http://127.0.0.1:9200/"})
        elasticindex {str} -- Index on ElasticSearch (default: {"twitter"})
    """

    es = Elasticsearch(elasticuri)

    if folder[-1] != "/":
        folder += "/"

    json_docs = []
    for filename in tqdm(os.listdir(folder)):
        if filename.endswith('.json'):
            with open(folder + filename) as open_file:
                cur_json = json.load(open_file)
                cur_json_id = cur_json['id']
                es.index(index=elasticindex,
                        doc_type='status',
                        id = cur_json_id,
                        body=cur_json)


if __name__ == '__main__':
    tprint("Full Load Json Dir")
    load_json_es()