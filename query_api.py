#!/usr/bin/env python
import io
import sys
import json
import logging
import os
import urllib
from random import choices
from time import sleep

import click
import pandas as pd
from dotenv import find_dotenv, load_dotenv

import arrow
import twitter
from elasticsearch import Elasticsearch, RequestError, helpers
from loguru import logger
from query_scrapper import scrape_twitter_by_date
from tools import dotter, hydratate_status, save_json, esK3K2_ascii_art
from tqdm import tnrange, tqdm
from scalpl import Cut

load_dotenv(find_dotenv(), verbose=True)

CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET= os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN_KEY= os.getenv("ACCESS_TOKEN_KEY")
ACCESS_TOKEN_SECRET= os.getenv("ACCESS_TOKEN_SECRET")
STATUSES_INDEX = "twitter"
TWITTER_DATETIME_PATTERN = "ddd MMM DD HH:mm:SS Z YYYY"
MAX_COUNT = 100
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)

@click.command()
@click.option('-q','--query', prompt='Your query', help='Twitter query. You can test it online with weh user interface of twitter.', required=True, type=str)
@click.option('-e','--elasticsearch_url', help='Elastic search uri f.e. http://127.0.0.1:9200 (default)', type=str , default="http://127.0.0.1:9200/")
@click.option('-x','--elasticsearch_index', help='Elastic search Index (default twitter)', type=str , default="twitter")
@click.option('-s','--since', help="'Since Status Id", default="0")
@click.option('-t','--time_sleep', help="'Time between twitter api requests in seconds (min 1.1 secs) ", type=float, default=1.1)
# TODO Add Authparameters
#click.option('-u','--elasticuser', help='Elastic search user (if authentication is needed)')
#click.option('-p','--elasticpass', help='Elastic search pass (if authentication is needed)')
def __query_api_statuses(query: str, elasticsearch_url: str, elasticuser: str = None, elasticpass: str = None, elasticsearch_index: str= STATUSES_INDEX, time_sleep: float = 1.1, since: str ='0'):
    """Goes to twitter API an get status info and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too
    
    Arguments:
        query {str} -- Proposed query to obtain statuses on Twitter
        elasticsearch_url {str} -- [description]
    
    Keyword Arguments:
        elasticuser {str} -- [description] (default: {None})
        elasticpass {str} -- [description] (default: {None})
        elasticsearch_index {str} -- [description] (default: {STATUSES_INDEX})
        since {str} -- Status ID to start twitter extraction (default: {'0'})        
    """
    return query_api_statuses(**locals())

def query_api_statuses(query: str, elasticsearch_url: str, elasticuser: str = None, elasticpass: str = None, elasticsearch_index: str= STATUSES_INDEX, time_sleep: float = 1.1, since: str ='0'):
    """Goes to twitter API an get status info and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too
    
    Arguments:
        query {str} -- Proposed query to obtain statuses on Twitter
        elasticsearch_url {str} -- [description]
    
    Keyword Arguments:
        elasticuser {str} -- [description] (default: {None})
        elasticpass {str} -- [description] (default: {None})
        elasticsearch_index {str} -- [description] (default: {STATUSES_INDEX})
        since {str} -- Status ID to start twitter extraction (default: {'0'})        
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

    api = twitter.Api(consumer_key=CONSUMER_KEY,
                  consumer_secret=CONSUMER_SECRET,
                  access_token_key=ACCESS_TOKEN_KEY,
                  access_token_secret=ACCESS_TOKEN_SECRET,
                  tweet_mode='extended')

    since_id = int(since)

    logger.info("Scrapping query on Twitter")

    df = scrape_twitter_by_date(query, start_date = arrow.now().format('YYYY-MM-DD'),  end_date=arrow.now().format('YYYY-MM-DD'))

    if df is not None:
        lst_statuses_ids = df['STATUS_ID'].tolist()
        hydrataded_statuses = hydratate_status(api, lst_statuses_ids)
    ## Save all jsons to file and load into Elastic
        logger.info("Processing Statuses from Twitter API to save jsons")
        for c_status_data in tqdm(hydrataded_statuses):
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
                logger.debug("Indexing: %s " % cur_id_str)
                es.index(index=elasticsearch_index,
                        #ignore=400,
                        doc_type='status',
                        id = cur_id_str,
                        body=cur_json)        

if __name__ == '__main__':
    esK3K2_ascii_art()
    print("Twitter Query API")
    __query_api_statuses()
