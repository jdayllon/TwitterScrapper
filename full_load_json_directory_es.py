#!/usr/bin/env python
from elasticsearch import Elasticsearch
import json
import os
from tqdm import tqdm
import click
from art import tprint

@click.command()
@click.option('-f','--folder', help='Folder with json files to load into ElasticSearch (default ./json/) ', type=str , default="./json/")
@click.option('-e','--elasticsearch_url', help='Elastic search uri f.e. http://127.0.0.1:9200 (default)', type=str , default="http://127.0.0.1:9200/")
@click.option('-x','--elasticsearch_index', help='Elastic search Index (default twitter)', type=str , default="twitter")
def load_json_es(folder: str = "./json/", elasticsearch_url: str = "http://127.0.0.1:9200/" , elasticsearch_index:str="twitter"):
    """Reads all json files in a folder an puts in a Elasticsearch index
    
    Keyword Arguments:
        folder {str} -- Folder that contains jsons (default: {"./json/"})
        elasticsearch_url {str} -- Uri to ElasticSearch (default: {"http://127.0.0.1:9200/"})
        elasticsearch_index {str} -- Index on ElasticSearch (default: {"twitter"})
    """

    es = Elasticsearch(elasticsearch_url)

    if folder[-1] != "/":
        folder += "/"

    json_docs = []
    for filename in tqdm(os.listdir(folder)):
        if filename.endswith('.json'):
            with open(folder + filename) as open_file:
                cur_json = json.load(open_file)
                cur_json_id = cur_json['id']
                es.index(index=elasticsearch_index,
                        doc_type='status',
                        id = cur_json_id,
                        body=cur_json)


if __name__ == '__main__':
    tprint("Full Load Json Dir")
    load_json_es()