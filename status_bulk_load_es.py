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
    job_es = Elasticsearch(job[1])
    job_idx = job[2]
    job_unshorturls = job[3]


    with open(job_file) as open_file:
        cur_json = json.load(open_file)
        cur_json_id = cur_json['id']
        cur_json = _prepare_json_status(cur_json, unshort_urls=job_unshorturls)
        job_es.index(index=job_idx,
                doc_type='status',
                id = cur_json_id,
                body=cur_json)

@click.command()
@click.option('-f','--folder', help='Folder with json files to load into ElasticSearch (default ./json/) ', type=str , default="./json/")
@click.option('-x','--elasticsearch_index', prompt="Insert index to load json files", help='ElasticSearch Index', type=str, required=True)
@click.option('-c','--config_file', help='YML file with the definition of settings and jobs', required=True, type=str, default = 'esK3K2_defaults.yml')
@click.option('-u/-n','--unshort/--no-unshort', help='Unshort or not urls, not unshorting is faster', default=False)
@click.option('-p/-l','--parallel/--linear', help='Parallelize load process or linear (one process)', default=True)
def load_json_es(folder: str, elasticsearch_index: str, config_file:str , unshort:bool=False, parallel:bool=True):
    """Reads all json files in a folder an puts in a Elasticsearch index
    
    Keyword Arguments:
        folder {str} -- Folder that contains jsons (default: {"./json/"})
        elasticsearch_index {str} -- Index on ElasticSearch (default: {"twitter"})
    """

    if folder[-1] != "/":
        folder += "/"

    settings = Settings()._load_config(config_file)

    jobs = []
    logger.info("Preparing Job list...")
    for filename in os.listdir(folder):
        if filename.endswith('.json'):
            jobs += [(folder + filename, settings.elasticsearch_url, elasticsearch_index, unshort)]
    
    if parallel:
        logger.info("Running Parallel Job list...")
        #!TODO Add n_jobs parameter
        parallel_process(jobs, _load_job_es, n_jobs=4)
    else:
        logger.info("Running linear execution job list...")
        for job in tqdm(jobs):
            _load_job_es(job)

if __name__ == '__main__':
    esK3K2_ascii_art()
    print("⚙️ Full Load Json 📂")    
    load_json_es()