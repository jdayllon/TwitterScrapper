#!/usr/bin/env python
from dotenv import load_dotenv, find_dotenv
import os
import twitter
from time import sleep
from elasticsearch import Elasticsearch,helpers,RequestError
from elasticsearch_dsl import Search
from random import choices
from scalpl import Cut
import pandas as pd
import json
import click
from tqdm import tnrange, tqdm
import logging
from loguru import logger
import arrow
import io
import sys
from tools import _prepare_json_status, dotter, save_json, esK3K2_ascii_art
from settings import Settings

load_dotenv(find_dotenv(), verbose=True)

CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET= os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN_KEY= os.getenv("ACCESS_TOKEN_KEY")
ACCESS_TOKEN_SECRET= os.getenv("ACCESS_TOKEN_SECRET")
STATUSES_INDEX = "twitter"

STEP= 100

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)
first_status_id = 0 

@click.command()
@click.option('-u','--user', prompt='User timeline', help='Get lastest 3k2 from timeline.', required=True, type=str)
@click.option('-t','--time_sleep', help="Time between twitter api requests in seconds (min 1.1 secs) ", type=float, default=1.1)
@click.option('-s','--since', help="'Since Status Id", default="0")
# TODO Add Authparameters
#click.option('-u','--elasticuser', help='Elastic search user (if authentication is needed)')
#click.option('-p','--elasticpass', help='Elastic search pass (if authentication is needed)')
def __download_api_timeline(user: str, time_sleep: float = 1.1, since: str ='0'):
    """Goes to twitter API an get timeline of a user_id and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too (Command line launch)
    
    Arguments:
        user {str} -- Twitter Screen Name
        elasticsearch_url {str} -- Base url of ElasticSearch
    
    Keyword Arguments:
        time_sleep {float} -- Time between requests (default: {1.1})
        since {str} -- Status ID to start twitter extraction (default: {'0'})        
    """

    return download_api_timeline(**locals())

def download_api_timeline(user: str, time_sleep: float = 1.1, since: str ='0'):
    """Goes to twitter API an get timeline of a user_id and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too
    
    Arguments:
        user {str} -- Twitter Screen Name
        elasticsearch_url {str} -- Base url of ElasticSearch
    
    Keyword Arguments:
        time_sleep {float} -- Time between requests (default: {1.1})
        since {str} -- Status ID to start twitter extraction (default: {'0'})        
    """

    global first_status_id

    settings = Settings()

    # Create a connection with Elastic
    if settings.ELASTICSEARCH_URL is not None:
        es = Elasticsearch(settings.ELASTICSEARCH_URL)
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


    # Go to Twitter API and get statuses by id
    logger.info("Downloading TimeLine Statuses from Twitter API")
    
    all_statuses_data = []
    logger.info("Starting at STATUS_ID: %s" % since)

    since_id = int(since)
    if since_id == 0:
        try:
            s = Search(using=es, index=STATUSES_INDEX, doc_type='status')
            s = s.query('match', user__screen_name=user)
            s = s.sort("-id","-_id")
            since_id = s.execute()[0]['id']
            logger.info("Starting 🐦 timeline for [%s] from: %d" % (user, since_id))
        except:
            logger.warning("Error retrieving last status from ES for [%s] using 0" % user)

    statuses = api.GetUserTimeline(screen_name=user, count=200, include_rts=True, exclude_replies=False, since_id = since_id)
    if len(statuses) == 0:
        print("%s" % since)
        logger.warning("There isn't new results for this Timeline")
        return since
    all_statuses_data += statuses
    last_status_id = statuses[-1].id
    first_status_id = statuses[0].id


    # If first status is equals to last in 'query' this means that all tweets has been readed
    while statuses[0].id != last_status_id and statuses is not []:
        statuses = api.GetUserTimeline(screen_name=user, count=200, include_rts=True, exclude_replies=False, max_id=last_status_id, since_id = since_id)
        logger.info("Readed: %d twts | Total: %d" % (len(statuses), len(all_statuses_data)))
        all_statuses_data += statuses
        last_status_id = statuses[-1].id
        sleep(time_sleep)

    all_statuses_data = set(all_statuses_data)
    
    # Save all jsons to file and load into Elastic
    logger.info("Processing TimeLine Statuses from 🐦 API to save jsons")
    for c_status_data in tqdm(all_statuses_data):
        cur_dict = Cut(c_status_data.AsDict())
        cur_id_str = cur_dict['id_str']

        # To ES improved data
        cur_json = _prepare_json_status(c_status_data)
        # TO FS orginal data backup
        cur_json_backup = json.dumps(cur_dict.data, indent=4)

        save_json(cur_json_backup,"./json/" + cur_id_str + ".json")

        if es is not None:
            es.index(index=settings.ELASTICSEARCH_STATUS_INDEX,
                    #ignore=400,
                    doc_type='status',
                    id = cur_id_str,
                    body=cur_json)
    # STDOut and STDErr
    print("%d" % first_status_id)
    return first_status_id

if __name__ == '__main__':
    esK3K2_ascii_art()
    print("Get'U'Timeline ")

    #logger.remove()
    #logger.add(sys.stderr, level="INFO")

    __download_api_timeline()