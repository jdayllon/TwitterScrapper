#!/usr/bin/env python
from loguru import logger
from unidecode import unidecode
from tqdm import tnrange, tqdm
from time import sleep

import re
import sys

STEP = 100

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
        logger.debug("Creating Path 🗂️:%s" % path)
        os.makedirs(path)
    except: 
        pass
    logger.debug("Creating File 🎫: %s" % filename)
    file = open(filename,'wb')
    file.write(json_string.encode())
    file.close()
    logger.debug("File created 🎫: %s" % filename)

# Based on https://stackoverflow.com/a/295466
def slugify(value):
    """Normalizes string, converts to lowercase, removes non-alpha characters, and converts spaces to hyphens.
    
    Arguments:
        value {str} -- String to transform in a slug
    
    Returns:
        str -- slugified version of input string
    """

    value = unidecode(value)
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    value = re.sub('[-\s]+', '-', value)
    
    return value    

def hydratate_status(api, statuses :list , time_sleep: float = 1.1):
    """ Gets from twitter API statuses full data (hydratate in twitter language :-D) from a list of status_id 
    
    Arguments:
        api {twitter api object} -- Instance of Twitter API
        statuses {list} -- Statuses dehydrated (list of ids)
        time_sleep {float} -- Time between requests
    """

    # Go to Twitter API and get statuses by id
    logger.info("Hydrating Statuses using 🐦 API")
    all_statuses_data = []
    for i in tqdm(range(0,len(statuses),STEP)):
        if i+STEP > len(statuses):
            cur_statuses = statuses[i:len(statuses)]
        else:
            cur_statuses = statuses[i:i+STEP]
        cur_statuses_data = api.GetStatuses(cur_statuses)
        all_statuses_data += cur_statuses_data
        sleep(time_sleep) 

    return all_statuses_data   