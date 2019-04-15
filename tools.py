#!/usr/bin/env python
from loguru import logger
from unidecode import unidecode
from tqdm import tnrange, tqdm
from time import sleep
from scalpl import Cut
from urllib.parse import urlparse
import arrow
import json
import geohash2
import numpy as np
import urlexpander
from concurrent.futures import ProcessPoolExecutor, as_completed
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
import botometer
from settings import Settings
from elasticsearch import Elasticsearch
from huey import RedisHuey

import re
import sys

STEP = 100
TWITTER_DATETIME_PATTERN = "ddd MMM DD HH:mm:SS Z YYYY"
FIXED_TWITTER_DATE_TIME = re.compile('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')
MAX_DELTA_BOTOMETER = 3600 * 24 * 2 # 2 Days
huey = RedisHuey('esK3K2')

def esK3K2_ascii_art():
    print("             __ _______ __ _____  ")
    print("  ___  _____/ //_/__  // //_/__ \ ")
    print(" / _ \/ ___/ ,<   /_ </ ,<  __/ / ")
    print("/  __(__  ) /| |___/ / /| |/ __/  ")
    print("\___/____/_/ |_/____/_/ |_/____/  ")
    print("                                  ")


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

REGEX_HREF = re.compile(r'href=\"([\w:\/\.-_]*)\"')

def _get_source_href(t: str):
    """Gets href from a tag, f.e. from 'source' field
    
    Arguments:
        t {str} -- tag string to get href
    
    Returns:
        [str] -- href from inside of tag if exists
    """
    
    href = REGEX_HREF.findall(t)
    if len(href) > 0:
        return href[0]
    else:
        return None

REGEX_HREF_DESC = re.compile(r'>([\w\s\.,-_:\/\\]*)<')

def _get_source_href_desc(t: str):
    """Gets description from a tag, f.e. from 'source' field
    
    Arguments:
        t {str} -- tag string to get href
    
    Returns:
        [str] -- href from inside of tag if exists
    """
    
    href_desc = REGEX_HREF_DESC.findall(t)
    if len(href_desc) > 0:
        return href_desc[0]
    else:
        return None

def __custom_filter(url):
    """This function returns True if the url is a shortened URL with custom addons like lajunta.es o opgob.es
    
    Arguments:
        url {str} -- Input url (to be determined if is a shortened url)
    
    Returns:
        [bool] -- if url is shortened
    """

    
    if urlexpander.get_domain(url) in ['lajunta.es','opgob.es','chng.it']:
        return True
    elif urlexpander.is_short(url):
        return True
    else:
        return False

@huey.task()
def get_status_unshorturls(status_id: int):
    status_data = _get_status_ES(status_id)
    if status_data is not None:
        proxy = Cut(status_data)
        unshorturls(proxy)
        _update_status_ES(status_id, proxy.data)
        return True
    else:
        return None

def unshorturls(proxy: Cut, batch=False):
    """Resolves shortened urls
    
    Arguments:
        proxy {Cut} -- Twitter status object (dict) under scalpl access

    
    Keyword Arguments:
        batch {bool} -- [description] (default: {False})
    
    Returns:
        [list] -- List of tuples with (shortened url, unshortened url, domain)
    """

    KEY_URLS = ['urls','media','quoted_status.media','quoted_status.urls','retweeted_status.media','retweeted_status.urls']
    shortened_urls = []

    for k in KEY_URLS:
        if k in proxy:
            for i in range(0,len(proxy[k])):
                cur_k_urls_expanded =  k + "[%d].expanded_url" % i
                if cur_k_urls_expanded in proxy:
                    c_url_expanded = proxy[cur_k_urls_expanded]
                    not_resolved = False
                    #if urlexpander.is_short(c_url_expanded) or "lajunta.es" in c_url_expanded:
                    if batch == False:
                        try:
                            proxy[cur_k_urls_expanded] = urlexpander.expand(c_url_expanded, filter_function=__custom_filter)
                        except:
                            not_resolved = True
                            logger.warning("I can't expand: %s" % proxy[cur_k_urls_expanded])
                    
                    if batch == False and not_resolved == False:
                        # Obtain a new key with only domain 
                        parsed_uri = urlparse(proxy[cur_k_urls_expanded])
                        domain = '{uri.netloc}'.format(uri=parsed_uri)
                        cur_k_urls_expanded_domain = cur_k_urls_expanded.replace('expanded_url','expanded_domain')
                        proxy[cur_k_urls_expanded_domain] = domain
                        shortened_urls += [(c_url_expanded,  proxy[cur_k_urls_expanded], domain)]
                    else:
                        shortened_urls += [(c_url_expanded,  None, None)]
    
    return shortened_urls
    


#https://stackoverflow.com/a/23021198
def centeroidnp(arr):
    length = arr.shape[0]
    sum_x = np.sum(arr[:, 0])
    sum_y = np.sum(arr[:, 1])
    return sum_x/length, sum_y/length


#http://danshiebler.com/2016-09-14-parallel-progress-bar/
def parallel_process(array, function, n_jobs=16, use_kwargs=False, front_num=3):
    """
        A parallel version of the map function with a progress bar. 

        Args:
            array (array-like): An array to iterate over.
            function (function): A python function to apply to the elements of array
            n_jobs (int, default=16): The number of cores to use
            use_kwargs (boolean, default=False): Whether to consider the elements of array as dictionaries of 
                keyword arguments to function 
            front_num (int, default=3): The number of iterations to run serially before kicking off the parallel job. 
                Useful for catching bugs
        Returns:
            [function(array[0]), function(array[1]), ...]
    """
    #We run the first few iterations serially to catch bugs
    if front_num > 0:
        front = [function(**a) if use_kwargs else function(a) for a in array[:front_num]]
    #If we set n_jobs to 1, just run a list comprehension. This is useful for benchmarking and debugging.
    if n_jobs==1:
        return front + [function(**a) if use_kwargs else function(a) for a in tqdm(array[front_num:])]
    #Assemble the workers
    with ProcessPoolExecutor(max_workers=n_jobs) as pool:
        #Pass the elements of array into function
        if use_kwargs:
            futures = [pool.submit(function, **a) for a in array[front_num:]]
        else:
            futures = [pool.submit(function, a) for a in array[front_num:]]
        kwargs = {
            'total': len(futures),
            'unit': 'it',
            'unit_scale': True,
            'leave': True
        }
        #Print out the progress as tasks complete
        for f in tqdm(as_completed(futures), **kwargs):
            pass
    out = []
    #Get the results from the futures. 
    for i, future in tqdm(enumerate(futures)):
        try:
            out.append(future.result())
        except Exception as e:
            out.append(e)
    return front + out

def translate(value,lang_pair='spa|eng'):
    """ Funcion que llama a Apertium APY para obtener una traduccion
        @value: cadena de entrada
        @lang_pair: identificador del patron de traduccion 
    """

    data = [
        ('langpair', lang_pair),
        ('q', value),
        ('markUnknown', 'no'),
    ]
    #!TODO Get YML Config Parameter with apertium url            
    res = requests.post('http://localhost:2737/translate', data=data)

    if res.status_code == 200:
        json_res = json.loads(res.content.decode('utf-8'))
        spa_chunk_str = Cut(json_res)['responseData.translatedText']
        
        return spa_chunk_str
    else:
        return None

@huey.task()
def get_status_sheldon_score(status_id: int):
    status_data = _get_status_ES(status_id)
    if status_data is not None:
        proxy = Cut(status_data)
        sheldon_score = get_sheldon_score(proxy['full_text'], proxy['lang'])
        proxy['sheldon_score'] = sheldon_score
        _update_status_ES(status_id, proxy.data)
        return True
    else:
        return None

def get_sheldon_score(sentence:str, lang:str = 'es'):
    #!TODO Do ISO Translations
    ISO_LANG = {
        'es':'spa',
        'en':'eng',
        'fr':'spa',
    }

    if lang not in ISO_LANG:
        return 0
    elif lang == 'es':
        base_lang = ISO_LANG[lang]
        logger.debug("Input Sentence: %s" % sentence)
        translated_text = translate(sentence ,'%s|eng' % base_lang)
        logger.debug("Translated Sentence: %s" % translated_text)
        sheldon_score = SentimentIntensityAnalyzer().polarity_scores(translated_text)['compound']
        logger.debug("Sentiment Score: %02f" % sheldon_score)
        return sheldon_score
    else:
        return 0

def _get_status_ES(status_id: int):

    settings = Settings()
    es = Elasticsearch(settings.ELASTICSEARCH_URL)

    if es.exists(index=settings.ELASTICSEARCH_STATUS_INDEX, doc_type='status', id=status_id):
        res = es.get(index=settings.ELASTICSEARCH_STATUS_INDEX, doc_type='status', id=status_id)
        return res['_source']
    else:
        return None

def _update_status_ES(status_id: int, json_data: dict):
    settings = Settings()
    es = Elasticsearch(settings.ELASTICSEARCH_URL)

    if es.exists(index=settings.ELASTICSEARCH_STATUS_INDEX, doc_type='status', id=status_id):
        _index_status_ES(status_id, json_data)
    else:
        return None

def _update_status_key_ES(status_id: int, key:str , data: dict):
    status_data = _get_status_ES(status_id)
    if status_data is not None:
        proxy = Cut(status_data)
        proxy[key] = data
        _update_status_ES(status_id, proxy.data)
        return True
    else:
        return None


def _index_status_ES(status_id: int, json_data: dict):
    settings = Settings()
    es = Elasticsearch(settings.ELASTICSEARCH_URL)

    logger.debug("Indexing: %s " % json_data['id'])
    es.index(index=settings.ELASTICSEARCH_STATUS_INDEX,
            doc_type='status',
            id = status_id,
            body=json.dumps(json_data))

@huey.task()
def _get_botometer_api_response(screen_name: str, status_id: int):

    # I can be that another task hast loader botometer fresh data (duplicated task)
    res = __check_botometer(screen_name)
    if res == False:
        settings = Settings()
        es = Elasticsearch(settings.ELASTICSEARCH_URL)

        twitter_app_auth = {
            'consumer_key': settings.CONSUMER_KEY,
            'consumer_secret': settings.CONSUMER_SECRET,
            'access_token': settings.ACCESS_TOKEN_KEY,
            'access_token_secret': settings.ACCESS_TOKEN_SECRET,
        }
        #! TODO look for a better place or documentate it
        logger.debug("Init Botometer API 🤖")
        botometer_api_url = 'https://botometer-pro.p.mashape.com'
        bom = botometer.Botometer(botometer_api_url=botometer_api_url,
                                wait_on_ratelimit=True,
                                mashape_key=settings.BOTOMETER_KEY,
                                **twitter_app_auth)
        logger.debug("Calling Botometer API 🤖: %s" % screen_name)
        res = Cut(bom.check_account(screen_name))
        res['updated_at'] = str(arrow.utcnow())
        cur_json = json.dumps(res.data,indent=4)

        save_json(cur_json, settings.BOTOMETER_JSON_BACKUP + "%s.json" % res['user.id_str'])

        logger.info("Indexing Botometer info for screen name: %s" % screen_name)
        es.index(index=settings.ELASTICSEARCH_BOT_INDEX,
                doc_type='res',
                id = screen_name,
                body=cur_json)

        logger.info("Indexing Status updated info for screen name: %s" % screen_name)
        _update_status_key_ES(status_id, 'user.botometer', res.data)
    elif type(res) == dict:
        _update_status_key_ES(status_id, 'user.botometer', res)
        
    
def __check_botometer(screen_name:str):
    """Calls ES API to get cached BotOMeter API scoring about a screen name if is a Bot or Not
    
    Arguments:
        screen_name {str} -- Twitter Screen Name
    
    Returns:
        [dict] -- BotOMeter API Response
    """    
    settings = Settings()

    es = Elasticsearch(settings.ELASTICSEARCH_URL)

    logger.debug("Checking ES Botometer Info 🤖: %s" % screen_name)
    if es.exists(index=settings.ELASTICSEARCH_BOT_INDEX, doc_type='res', id=screen_name):
        logger.debug("Botometer Info 🤖found for: %s" % screen_name)
        res = es.get(index=settings.ELASTICSEARCH_BOT_INDEX, doc_type='res', id=screen_name)
        delta = arrow.utcnow() - arrow.get(res['_source']['updated_at'])
        
        if MAX_DELTA_BOTOMETER < delta.total_seconds():
            logger.debug("Deprecated ES Botometer Info 🤖: %s" % screen_name)
            return False
        else:
            logger.debug("Found ES Botometer Info 🤖: %s" % screen_name)
            return res['_source']
    else:
        logger.debug("NOT Found ES Botometer Info 🤖: %s" % screen_name)
        return False
            

def get_botometer(screen_name: str, status_id: int):
    """Checks if ES has info about Botometer user info, if not establish a task to retrieve it and update current status on ES
    
    Arguments:
        screen_name {str} -- Twitter Screen Name
        status_id {int} -- Twitter Status id
    
    Returns:
        [dict] -- Botometer API Rest from ES
    """

    res = __check_botometer(screen_name)
    if type(res) is dict:
        logger.debug("Botometer Info 🤖found for: %s" % screen_name)
        return res
    else:
        _get_botometer_api_response(screen_name, status_id)
        return None
    #settings = Settings()

    #es = Elasticsearch(settings.ELASTICSEARCH_URL)
    #if es.exists(index=settings.ELASTICSEARCH_BOT_INDEX, doc_type='res', id=screen_name):
    #    logger.debug("Botometer Info 🤖found for: %s" % screen_name)
    #    res = es.get(index=settings.ELASTICSEARCH_BOT_INDEX, doc_type='res', id=screen_name)
    #    delta = arrow.utcnow() - arrow.get(res['_source']['updated_at'])
    #    
    #    if MAX_DELTA_BOTOMETER < delta.total_seconds():
    #        logger.info("Botometer Info 🤖 older than delta, retrieving again: %s" % screen_name)
    #        _get_botometer_api_response(screen_name, status_id)
    #    else:
    #        return res['_source']    
    #else:
    #    logger.info("Botometer Info 🤖 not found for: %s" % screen_name)
    #    _get_botometer_api_response(screen_name, status_id)
    #    return None

def _prepare_json_status(status, unshort_urls=True, sheldon=True):
    """ Improves status info from Twitter and fixes for better indexing
    
    Arguments:
        status {dict} -- Dict with Twitter Status
    
    Keyword Arguments:
        unshort_urls {bool} -- Enables/Disables URLs, if is disables load its much faster (no url resolution) (default: {True})
    
    Returns:
        [str] -- Json output of Status postprocessed
    """

    settings = Settings()

    if type(status) is dict:
        proxy = Cut(status)
    else:
        proxy = Cut(status.AsDict())
    id_str = proxy['id_str']

    # Fix twitter dates to more 'standart' date format
    list_all_keys_w_dots = dotter(proxy.data,'',[])
    try:
        for created_at_keys in list_all_keys_w_dots:
            if 'created_at' in created_at_keys:
                # If matches this means that is fixed in earlier process
                if FIXED_TWITTER_DATE_TIME.match(proxy[created_at_keys]) is None:
                    cur_dt = arrow.get(proxy[created_at_keys], TWITTER_DATETIME_PATTERN)
                    proxy[created_at_keys] = cur_dt.format("YYYY-MM-DDTHH:MM:SS")+"Z"
    except:
        import ipdb; ipdb.set_trace()
        logger.warning("Error parsing dates on %s" % id_str)

    # Fixed source
    try:
        proxy["source_href"] = _get_source_href(proxy["source"])
        proxy["source_desc"] = _get_source_href_desc(proxy["source"])
    except:
        logger.warning("Error fixing source getting href")

    # Fixed geolocations
    # If len(proxy['xxxx.coordinates']) == 1 means that don't have lat , lot, probably a geohash

    # Twitter api says:
    # The longitude and latitude of the Tweet’s location, as a collection in the form [longitude, latitude]. Example: "coordinates":[-97.51087576,35.46500176]
    # Geohash lib input function : def encode(latitude, longitude, precision=12):

    if 'coordinates.coordinates' in proxy and len(proxy['coordinates.coordinates']) > 1:
        proxy['coordinates.coordinates'] = geohash2.encode(proxy['coordinates.coordinates'][1], proxy['coordinates.coordinates'][0])
    if 'geo.coordinates' in proxy and len(proxy['geo.coordinates']) > 1:
        proxy['geo.coordinates'] = geohash2.encode(proxy['geo.coordinates'][0], proxy['geo.coordinates'][1])

    KEY_PLACE_BB = 'place.bounding_box.coordinates'

    if KEY_PLACE_BB in proxy:

        centroid_bb_data = []

        for i in range(0,len(proxy[KEY_PLACE_BB])):
            for j in range(0,len(proxy[KEY_PLACE_BB + "[%d]" % i])):
                cur_key_bb_ij = KEY_PLACE_BB + "[%d][%d]" % (i,j)
                c_lat = proxy[cur_key_bb_ij][1]
                c_lon = proxy[cur_key_bb_ij][0]
                centroid_bb_data += [(float(c_lat),float(c_lon))]
                proxy[cur_key_bb_ij] =  geohash2.encode(c_lat, c_lon)
        
        # Create a new point with de centroid
        if len(centroid_bb_data) > 0:
            centroid_bb_arr = np.array(centroid_bb_data)
            centroid_bb = centeroidnp(centroid_bb_arr)
            proxy['place.bounding_box_centroid'] = geohash2.encode(centroid_bb[0], centroid_bb[1])

    # Check and fix shortened urls in expanded field:
    if unshort_urls:
        get_status_unshorturls(proxy['id'])
    
    # Get sheldon score
    if sheldon:
        #! TODO Transform ISO
        #proxy['sheldon_score'] = get_sheldon_score(proxy['full_text'], proxy['lang'], proxy['id'])
        get_status_sheldon_score(proxy['id'])

    # Get Botometer
    #! TODO Add ES search for botometer info
    if settings.BOTOMETER_KEY is not None:
        proxy['user.botometer'] = get_botometer(proxy['user.screen_name'], proxy['id'] )

    return json.dumps(proxy.data, indent=4)    
