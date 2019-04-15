#!/usr/bin/env python
from elasticsearch import Elasticsearch
import json
import os
from tqdm import tqdm
import click
from loguru import logger
from tools import _prepare_json_status, parallel_process, esK3K2_ascii_art
from settings import Settings
from scalpl import Cut
import arrow

def _load_job_es(job):

    job_file = job[0]
    job_es = job[1]
    job_idx = job[2]

    with open(job_file) as open_file:
        cur_json = Cut(json.load(open_file))
        cur_json['updated_at'] = str(arrow.utcnow()) #arrow.get().format("YYYY-MM-DDTHH:MM:SS")+"Z"
        cur_json_id = cur_json['user.screen_name']

        job_es.index(index=job_idx,
                doc_type='res',
                id = cur_json_id,
                body=json.dumps(cur_json.data))

@click.command()
@click.option('-f','--folder', help='Folder with json files to load into ElasticSearch (default ./json/) ', type=str , default="./json/")
@click.option('-c','--config_file', help='YML file with the definition of settings and jobs', required=True, type=str, default = 'esK3K2_defaults.yml')
def load_json_es(folder: str, config_file:str):
    """Reads all json files in a folder an puts in a Elasticsearch index
    
    Keyword Arguments:
        folder {str} -- Folder that contains jsons (default: {"./json/"})
        elasticsearch_index {str} -- Index on ElasticSearch (default: {"twitter"})
    """

    if folder[-1] != "/":
        folder += "/"

    settings = Settings()._load_config(config_file)

    es = Elasticsearch(settings.elasticsearch_url)

    es.indices.delete(settings.elasticsearch_bot_index)
    if not es.indices.exists(settings.elasticsearch_bot_index):
        # Create new index 
        es.indices.create(settings.elasticsearch_bot_index)

    jobs = []
    logger.info("Preparing Job list...")
    for filename in os.listdir(folder):
        if filename.endswith('.json'):
            jobs += [(folder + filename, es, settings.elasticsearch_bot_index)]
    
    for job in tqdm(jobs):
        _load_job_es(job)

if __name__ == '__main__':
    esK3K2_ascii_art()
    print("⚙️ Full Load Json Botometer API 🤖")    
    load_json_es()