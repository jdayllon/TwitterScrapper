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

import arrow
import twitter
from elasticsearch import Elasticsearch, RequestError, helpers
from loguru import logger
from query_scrapper import scrape_twitter_by_date
from tools import dotter, hydratate_status, save_json, esK3K2_ascii_art, _prepare_json_status
from tqdm import tnrange, tqdm
from scalpl import Cut
from settings import Settings

STATUSES_INDEX = "twitter"
#TWITTER_DATETIME_PATTERN = "ddd MMM DD HH:mm:SS Z YYYY"
MAX_COUNT = 100
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)

@click.command()
@click.option('-u','--user', prompt='Enter Screen Name', help='🐦 Twitter Screen Name', required=True, type=str)
@click.option('-c','--config_file', help='YML file with the definition of settings and jobs', required=True, type=str, default = 'esK3K2_defaults.yml')
@click.option('-s','--start_date', help='This script scrappes tweeter by day by day, so you need to set a start date. Format: YYYY-MM-DD')
@click.option('-e','--end_date', help='This script scrappes tweeter by day by day, so you need to set a end date. Format: YYYY-MM-DD')
def __query_user(user: str, config_file: str, start_date:str=arrow.get().format('YYYY-MM-DD'), end_date:str=arrow.get().shift(years=-10).format('YYYY-MM-DD'), time_sleep: float = 1.1):
    return query_user(**locals())

def query_user(user: str, config_file: str, start_date:str=arrow.get().format('YYYY-MM-DD'), end_date:str=arrow.get().shift(years=-10).format('YYYY-MM-DD'), time_sleep: float = 1.1):
    """Get Twitter information about user profiles
    
    Arguments:
        user {str} -- Screen name about twitter
        config_file {str} -- Config yml file
    
    Keyword Arguments:
        time_sleep {float} -- [description] (default: {1.1})
        start_date {str} -- Start date from being requested a twitter user (default: {arrow.get().format('YYYY-MM-DD')})
        end_date {str} -- End date from being requested a twitter user (default: {arrow.get().shift(years=-10).format('YYYY-MM-DD')})
    
    Raises:
        err -- Error when time sleeps is less than 1.1
    """
    settings = Settings()._load_config(config_file)

    # Create a connection with Elastic
    if settings.elasticsearch_url is not None:
        es = Elasticsearch(settings.elasticsearch_url)
        logger.info(es.info())
    else:
        es = None

    # Check if time_sleep is more than 1.1 secs
    try:
        assert time_sleep >= 1.1
    except:
        logger.error("Time Sleep less than 1.1 secs (minimum) ")
        raise err

    api = twitter.Api(consumer_key=settings.CONSUMER_KEY,
                  consumer_secret=settings.CONSUMER_SECRET,
                  access_token_key=settings.ACCESS_TOKEN_KEY,
                  access_token_secret=settings.ACCESS_TOKEN_SECRET,
                  tweet_mode='extended')

    user_query = "to:%s OR from:%s OR on:%s" % (user, user, user)

    logger.info("Scrapping query on Twitter")

    df = scrape_twitter_by_date(user_query, start_date,end_date)

    if df is not None:
        lst_statuses_ids = df['STATUS_ID'].tolist()
        hydrataded_statuses = hydratate_status(api, lst_statuses_ids)
        ## Save all jsons to file and load into Elastic
        logger.info("Processing Statuses from Twitter API to save jsons")
        for c_status_data in tqdm(hydrataded_statuses):
            cur_dict = Cut(c_status_data.AsDict())
            cur_id_str = cur_dict['id_str']

            #To ES improved data
            cur_json = _prepare_json_status(cur_dict.data)
            # TO FS orginal data backup
            cur_json_backup = json.dumps(cur_dict.data, indent=4)

            save_json(cur_json_backup,settings.status_json_backup + cur_id_str + ".json")

            if es is not None:
                logger.debug("Indexing: %s " % cur_id_str)
                es.index(index=settings.ELASTICSEARCH_STATUS_INDEX,
                        doc_type='status',
                        id = cur_id_str,
                        body=cur_json)        

if __name__ == '__main__':
    esK3K2_ascii_art()
    print("Twitter 🐦 Query 🗣 User")

    logger.remove()
    logger.add(sys.stderr, level="INFO")    

    __query_user()