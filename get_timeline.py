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
import sys
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
first_status_id = 0 

def dotter(d: dict, key, dots):
    """Creates a list of key of a dict is very useful to use with Scalpl
    Arguments:
        d {dict} -- Input dictionary
        key {[type]} -- Recursive keys
        dots {[type]} -- Recursive return values
    
    Returns:
        [type] -- [description]
    """
    #* Comes from a fef in stakoverflow https://stackoverflow.com/a/29689275
    
    if isinstance(d, dict):
        for k in d:
            dotter(d[k], key + '.' + k if key else k, dots)
    else:
        dots.append(key)

    return dots

def save_json(json_string: str, filename: str):
    """Saves a non-compressed object to disk
    
    Arguments:
        json_string {str} -- String with a json 
        filename {str} -- Output filename
    
    """
    filename_only = filename.split('/')[-1]
    path = "/".join(filename.split("/")[:-1]) + "/"
    try:
        logger.debug("Creatinh Path:%s" % path)
        os.makedirs(path)
    except: 
        pass
    logger.debug("Creating File: %s" % filename)
    file = open(filename,'wb')
    file.write(json_string.encode())
    file.close()
    logger.debug("File created: %s" % filename)


@click.command()
@click.option('-u','--user', prompt='User timeline', help='Get lastest 3k2 from timeline.', required=True, type=str)
@click.option('-e','--elasticsearch_url', help='Elastic search uri f.e. http://127.0.0.1:9200', type=str )
@click.option('-x','--elasticsearch_index', help='Elastic search Index (default twitter)', type=str , default="twitter")
@click.option('-t','--time_sleep', help="Time between twitter api requests in seconds (min 1.1 secs) ", type=float, default=1.1)
@click.option('-s','--since', help="'Since Status Id", default="0")
# TODO Add Authparameters
#click.option('-u','--elasticuser', help='Elastic search user (if authentication is needed)')
#click.option('-p','--elasticpass', help='Elastic search pass (if authentication is needed)')
def __download_api_timeline(user: str, elasticsearch_url: str, elasticuser: str = None, elasticpass: str = None, elasticsearch_index: str= STATUSES_INDEX, time_sleep: float = 1.1, since: str ='0'):
    """Goes to twitter API an get timeline of a user_id and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too (Command line launch)
    
    Arguments:
        user {str} -- Twitter Screen Name
        elasticsearch_url {str} -- Base url of ElasticSearch
    
    Keyword Arguments:
        elasticuser {str} -- Elastic User - Not Used (default: {None})
        elasticpass {str} -- Elastic Password - Not Used (default: {None})
        elasticsearch_index {str} -- Id of ElasticSearch Index (default: {STATUSES_INDEX})
        time_sleep {float} -- Time between requests (default: {1.1})
        since {str} -- Status ID to start twitter extraction (default: {'0'})        
    """

    return download_api_timeline(**locals())

def download_api_timeline(user: str, elasticsearch_url: str, elasticuser: str = None, elasticpass: str = None, elasticsearch_index: str= STATUSES_INDEX, time_sleep: float = 1.1, since: str ='0'):
    """Goes to twitter API an get timeline of a user_id and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too
    
    Arguments:
        user {str} -- Twitter Screen Name
        elasticsearch_url {str} -- Base url of ElasticSearch
    
    Keyword Arguments:
        elasticuser {str} -- Elastic User - Not Used (default: {None})
        elasticpass {str} -- Elastic Password - Not Used (default: {None})
        elasticsearch_index {str} -- Id of ElasticSearch Index (default: {STATUSES_INDEX})
        time_sleep {float} -- Time between requests (default: {1.1})
        since {str} -- Status ID to start twitter extraction (default: {'0'})        
    """

    global first_status_id

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

    # Go to Twitter API and get statuses by id
    logger.info("Downloading TimeLine Statuses from Twitter API")
    
    all_statuses_data = []
    logger.info("Starting at STATUS_ID: %s" % since)

    since_id = int(since)

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
    logger.info("Processing TimeLine Statuses from Twitter API to save jsons")
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
    # STDOut and STDErr
    print("%d" % first_status_id)
    return first_status_id

if __name__ == '__main__':
    tprint("Twitter 3k2 Timeline ")
    __download_api_timeline()