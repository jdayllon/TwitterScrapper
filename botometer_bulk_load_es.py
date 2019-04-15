#!/usr/bin/env python
from elasticsearch import Elasticsearch
import json
import os
from tqdm import tqdm
import click
from loguru import logger
from tools import _prepare_json_status, parallel_process, esK3K2_ascii_art
from settings import Settings

def _load_job_es(job):

    job_file = job[0]
    job_es = job[1]
    job_idx = job[2]
    job_unshorturls = job[4]


    with open(job_file) as open_file:
        cur_json = json.load(open_file)
        cur_json_id = cur_json['id']
        cur_json = _prepare_json_status(cur_json, unshort_urls=job_unshorturls)
        job_es.index(index=job_idx,
                doc_type='res',
                id = cur_json_id,
                body=cur_json)

@click.command()
@click.option('-f','--folder', help='Folder with json files to load into ElasticSearch (default ./json/) ', type=str , default="./json/")
@click.option('-c','--config_file', help='YML file with the definition of settings and jobs', required=True, type=str, default = 'esK3K2_defaults.yml')
def load_json_es(folder: str, config_file:str , unshort:bool=False, parallel:bool=True):
    """Reads all json files in a folder an puts in a Elasticsearch index
    
    Keyword Arguments:
        folder {str} -- Folder that contains jsons (default: {"./json/"})
        elasticsearch_index {str} -- Index on ElasticSearch (default: {"twitter"})
    """

    if folder[-1] != "/":
        folder += "/"

    settings = Settings()._load_config(config_file)

    es = Elasticsearch(settings.elasticsearch_url)

    import ipdb; ipdb.set_trace()

    if not es.indices.exists(settings.elasticsearch_bot_index):
        # Create new index 
        es.indices.create(settings.elasticsearch_bot_index)

    jobs = []
    logger.info("Preparing Job list...")
    for filename in os.listdir(folder):
        if filename.endswith('.json'):
            jobs += [(folder + filename, es, settings.elasticsearch_bot_index, unshort)]
    
    for job in tqdm(jobs):
        _load_job_es(job)

if __name__ == '__main__':
    esK3K2_ascii_art()
    print("⚙️ Full Load Json 🤖")    
    load_json_es()