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

    es = Elasticsearch(elasticuri)

    if folder[-1] != "/":
        folder += "/"

    json_docs = []
    for filename in tqdm(os.listdir(folder)):
        if filename.endswith('.json'):
            with open(folder + filename) as open_file:
                es.index(index=elasticindex,
                        doc_type='status',
                        body=json.load(open_file))


if __name__ == '__main__':
    tprint("Full Load Json Dir")
    load_json_es()