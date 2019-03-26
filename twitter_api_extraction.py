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
import arrow
from art import tprint
import io
load_dotenv(find_dotenv(), verbose=True)

CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET= os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN_KEY= os.getenv("ACCESS_TOKEN_KEY")
ACCESS_TOKEN_SECRET= os.getenv("ACCESS_TOKEN_SECRET")
STATUSES_INDEX = "twitter"
TWITTER_DATETIME_PATTERN = "ddd MMM DD HH:mm:SS Z YYYY"
STEP= 100
logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger("tools")
logger.setLevel(logging.INFO)
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)


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
@click.option('-i','--input', help='Input file in MSGPACK format with a column named STATUS_ID with Twitter STATUS_ID ;-)', required=True, type=str)
@click.option('-e','--elasticuri', help='Elastic search uri f.e. http://127.0.0.1:9200 (default)', type=str , default="http://127.0.0.1:9200/")
@click.option('-x','--elasticindex', help='Elastic search Index (default twitter)', type=str , default="twitter")
# TODO Add Authparameters
#click.option('-u','--elasticuser', help='Elastic search user (if authentication is needed)')
#click.option('-p','--elasticpass', help='Elastic search pass (if authentication is needed)')
def download_api_statuses(input: str, elasticuri: str, elasticuser: str = None, elasticpass: str = None, elasticindex: str= STATUSES_INDEX):
    """Goes to twitter API an get status info and saves into a json file (in "json" dir) and if Elasticsearch is identified send it too
    
    Arguments:
        input {str} -- [description]
        elasticuri {str} -- [description]
    
    Keyword Arguments:
        elasticuser {str} -- [description] (default: {None})
        elasticpass {str} -- [description] (default: {None})
        elasticindex {str} -- [description] (default: {STATUSES_INDEX})
    """


    # Create a connection with Elastic
    if elasticuri is not None:
        es = Elasticsearch(elasticuri)
        logger.info(es.info())
    else:
        es = None
    
    # Read MSGPACK file whith statuses id
    df = pd.read_msgpack(input)

    api = twitter.Api(consumer_key=CONSUMER_KEY,
                  consumer_secret=CONSUMER_SECRET,
                  access_token_key=ACCESS_TOKEN_KEY,
                  access_token_secret=ACCESS_TOKEN_SECRET,
                  tweet_mode='extended')

    all_statuses_id = df['STATUS_ID'].tolist()
    
    # Go to Twitter API and get statuses by id
    logger.info("Downloading Statuses from Twitter API")
    all_statuses_data = []
    for i in tqdm(range(0,len(all_statuses_id),STEP)):
        if i+STEP > len(all_statuses_id):
            cur_statuses = all_statuses_id[i:len(all_statuses_id)]
        else:
            cur_statuses = all_statuses_id[i:i+STEP]
        cur_statuses_data = api.GetStatuses(cur_statuses)
        all_statuses_data += cur_statuses_data
        sleep(1.1)
    
    # Save all jsons to file and load into Elastic
    logger.info("Processing Statuses from Twitter API to save jsons")
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
            es.index(index=elasticindex,
                    #ignore=400,
                    doc_type='status',
                    id = cur_id_str,
                    body=cur_json)
    

if __name__ == '__main__':
    tprint("Twitter API Extraction")
    download_api_statuses()