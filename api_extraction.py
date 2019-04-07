#!/usr/bin/env python
from dotenv import load_dotenv, find_dotenv
import os
import twitter
from time import sleep
from elasticsearch import Elasticsearch,helpers,RequestError
from random import choices
from scalpl import Cut
import pandas as pd
import json
import click
from tqdm import tnrange, tqdm
import logging
from loguru import logger
import arrow
from art import tprint
import io

from tools import hydratate_status, dotter, save_json

load_dotenv(find_dotenv(), verbose=True)

CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET= os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN_KEY= os.getenv("ACCESS_TOKEN_KEY")
ACCESS_TOKEN_SECRET= os.getenv("ACCESS_TOKEN_SECRET")
STATUSES_INDEX = "twitter"
TWITTER_DATETIME_PATTERN = "ddd MMM DD HH:mm:SS Z YYYY"
STEP= 100
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)

@click.command()
@click.option('-i','--input', help='Input file in MSGPACK format with a column named STATUS_ID with Twitter STATUS_ID ;-)', required=True, type=str)
@click.option('-e','--elasticsearch_url', help='Elastic search uri f.e. http://127.0.0.1:9200 (default)', type=str , default="http://127.0.0.1:9200/")
@click.option('-x','--elasticsearch_index', help='Elastic search Index (default twitter)', type=str , default="twitter")
@click.option('-t','--time_sleep', help="'Time between twitter api requests in seconds (min 1.1 secs) ", type=float, default=1.1)
# TODO Add Authparameters
#click.option('-u','--elasticuser', help='Elastic search user (if authentication is needed)')
#click.option('-p','--elasticpass', help='Elastic search pass (if authentication is needed)')
def download_api_statuses(input: str, elasticsearch_url: str, elasticuser: str = None, elasticpass: str = None, elasticsearch_index: str= STATUSES_INDEX, time_sleep: float = 1.1):
    """Goes to twitter API an get status info (hydratated) and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too
    
    Arguments:
        input {str} -- [description]
        elasticsearch_url {str} -- [description]
    
    Keyword Arguments:
        elasticuser {str} -- [description] (default: {None})
        elasticpass {str} -- [description] (default: {None})
        elasticsearch_index {str} -- [description] (default: {STATUSES_INDEX})
    """


    # Create a connection with Elastic
    if elasticsearch_url is not None:
        es = Elasticsearch(elasticsearch_url)
        logger.info(es.info())
    else:
        es = None

    # Check if time_sleep is more than 1.1 secs
    try:
        assert time_sleep >= 1.1
    except:
        logger.error("Time Sleep less than 1.1 secs (minimum) ")
        raise err
    # Read MSGPACK file whith statuses id
    df = pd.read_msgpack(input)

    api = twitter.Api(consumer_key=CONSUMER_KEY,
                  consumer_secret=CONSUMER_SECRET,
                  access_token_key=ACCESS_TOKEN_KEY,
                  access_token_secret=ACCESS_TOKEN_SECRET,
                  tweet_mode='extended')

    all_statuses_id = df['STATUS_ID'].tolist()
    
    # Go to Twitter API and get statuses by id
    logger.info("Downloading Statuses from 🐦 API")
    all_statuses_data = hydratate_status(api, all_statuses_id, time_sleep)

    # Save all jsons to file and load into Elastic
    logger.info("Processing Statuses from 🐦 API to save jsons")
    for c_status_data in tqdm(all_statuses_data):
        cur_dict = Cut(c_status_data.AsDict())
        cur_id_str = cur_dict['id_str']

        # Fix twitter dates to more 'standart' date format
        list_all_keys_w_dots = dotter(cur_dict.data,'',[])
        try:
            for created_at_keys in list_all_keys_w_dots:
                if 'created_at' in created_at_keys:
                    cur_dt = arrow.get(cur_dict[created_at_keys], TWITTER_DATETIME_PATTERN)
                    cur_dict[created_at_keys] = cur_dt.format("YYYY-MM-DDTHH:MM:SS")+"Z"
        except:
            logger.error("Error parsing dates on %s" % cur_id_str)

        cur_json = json.dumps(cur_dict.data, indent=4)

        save_json(cur_json,"./json/" + cur_id_str + ".json")
        if es is not None:
            es.index(index=elasticsearch_index,
                    #ignore=400,
                    doc_type='status',
                    id = cur_id_str,
                    body=cur_json)
    

if __name__ == '__main__':
    tprint("🐦 API Extraction")
    download_api_statuses()