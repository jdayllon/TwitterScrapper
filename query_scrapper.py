#!/usr/bin/env python
import requests
import arrow
from scalpl import Cut
from bs4 import BeautifulSoup
from tqdm import tqdm, tnrange
import pandas as pd
import pickle
import click
import re
import urllib
from art import tprint
import logging
from tools import slugify
from loguru import logger

#logging.basicConfig(format=logFormatter, level=logging.INFO)
#logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
#logger = logging.getLogger("tools")
#logger.setLevel(logging.INFO)

def get_search_url_by_day(q, date):
    
    search_url = '%s since:%s until:%s' % (q, date.format('YYYY-MM-DD'), date.shift(days=1).format('YYYY-MM-DD'))
    search_url = 'https://mobile.twitter.com/search?q=' + urllib.parse.quote_plus(search_url)
    return search_url

def _get_status(soup):
    c_statuses = []
    tweets = soup.find_all('table', {"class": "tweet"})
    #print("Tweets found: %d" % len(tweets))
    for cur_tweet in tweets:

        if 'tombstone-tweet' in cur_tweet['class']:
            # Dead twitter account reference  
            continue

        #soup.find_all('div', {"class": "tweet-text"}):
        cur_tweet_data = cur_tweet.find('div', {"class": "tweet-text"})
        try:

            cur_tweet_text = cur_tweet_data.find('div', {"class": "dir-ltr"})
            if cur_tweet_text is None:

                cur_tweet_text = cur_tweet_data.get_text()
            else:
                cur_tweet_text = cur_tweet_text.get_text()
            cur_tweet_date = cur_tweet.find('td', {"class": "timestamp"}).find('a').get_text()

            if "h" in cur_tweet_date and len(cur_tweet_date) < 4:
                hours = int(re.findall("([0-9]{0,2})\s?h", cur_tweet_date)[0])
                cur_tweet_date = arrow.get().shift(hours=-hours).format("YYYY-MM-DD")
            elif "m" in cur_tweet_date and len(cur_tweet_date) < 4:
                hours = int(re.findall("([0-9]{0,2})\s?m", cur_tweet_date)[0])
                cur_tweet_date = arrow.get().shift(hours=-hours).format("YYYY-MM-DD")
            elif "s" in cur_tweet_date and len(cur_tweet_date) < 4:
                hours = int(re.findall("([0-9]{0,2})\s?s", cur_tweet_date)[0])
                cur_tweet_date = arrow.get().shift(hours=-hours).format("YYYY-MM-DD")
            elif len(cur_tweet_date) <9:
                # On current year tweets doesn't show a year in text
                cur_tweet_date += arrow.get().format(" YY")
                cur_tweet_date = arrow.get(cur_tweet_date,"MMM D YY").format("YYYY-MM-DD")
            else:
                cur_tweet_date = arrow.get(cur_tweet_date,"D MMM YY").format("YYYY-MM-DD")
            c_statuses += [(cur_tweet_data['data-id'], 
                            cur_tweet['href'], 
                            cur_tweet_date,
                            cur_tweet_text)]
        except:
            logger.warn ("Not processing: \n %s" % cur_tweet)
    return c_statuses    


def _get_next_page_link(soup):
    """Search above soup object next page button to return link if no link found returns None
    
    Arguments:
        soup {BeautifulSoup} -- Object with html parsed content
    
    Returns:
        str -- URL with next page (or None if isn't found)
    """

    next_link = soup.find_all('div', {"class": "w-button-more"})
    if len(next_link) > 0:
        next_page_link = "https://mobile.twitter.com" + next_link[0].find('a')['href']
        return next_page_link
    else:
        return None

@click.command()
@click.option('-q','--query', prompt='Your query', help='Twitter query. You can test it online with weh user interface of twitter.', required=True, type=str)
@click.option('-s','--start_date', help='This script scrappes tweeter by day by day, so you need to set a start date. Format: YYYY-MM-DD')
@click.option('-e','--end_date', help='This script scrappes tweeter by day by day, so you need to set a end date. Format: YYYY-MM-DD')
def __scape_twitter_by_date(query: str, start_date:str=arrow.get().format('YYYY-MM-DD'), end_date:str=arrow.get().shift(years=-10).format('YYYY-MM-DD')):
    """Simple program that greets NAME for a total of COUNT times. (Command line launch)
    
    Arguments:
        query {str} -- Twitter query language expression (can be tested on twitter)
    
    Keyword Arguments:
        start_date {str} -- Start date from being requested a twitter query (default: {arrow.get().format('YYYY-MM-DD')})
        end_date {str} -- End date from being requested a twitter query  (default: {arrow.get().shift(years=-10).format('YYYY-MM-DD')})
    """

    df = scape_twitter_by_date(**locals())
    if df is not None:

        file_start_date = arrow.get(start_date).format('YYYYMMDD')
        file_end_date = arrow.get(end_date).format('YYYYMMDD')         
        
        output_filename = "%s_%s--%s.msg" % (file_start_date, file_end_date,slugify(query))
        logger.info("Saving results into : %s" % output_filename )
        df.to_msgpack(output_filename)


def scape_twitter_by_date(query: str, start_date:str=arrow.get().format('YYYY-MM-DD'), end_date:str=arrow.get().shift(years=-10).format('YYYY-MM-DD')):
    """Simple program that greets NAME for a total of COUNT times.
    
    Arguments:
        query {str} -- Twitter query language expression (can be tested on twitter)
    
    Keyword Arguments:
        start_date {str} -- Start date from being requested a twitter query (default: {arrow.get().format('YYYY-MM-DD')})
        end_date {str} -- End date from being requested a twitter query  (default: {arrow.get().shift(years=-10).format('YYYY-MM-DD')})
    """
    cur_date = arrow.get(start_date) 
    finish_date = arrow.get(end_date) 

    logger.info("Scrapping 🐦 with:[%s] From 🗓️:[%s] ➡️ To 🗓️:[%s]" % (query, cur_date.format('YYYY-MM-DD'), finish_date.format('YYYY-MM-DD')))

    # Create day urls
    urls = []
    while cur_date <= finish_date:
        urls += [get_search_url_by_day(query, cur_date)]
        cur_date = cur_date.shift(days=+1)

    logger.info("Num requests to send: %d" % len(urls))

    statuses = []
    for c_url in tqdm(urls):
        res = requests.get(c_url)

        soup = BeautifulSoup(res.content,"html.parser")
        statuses += _get_status(soup)

        next_c_url = _get_next_page_link(soup)
        while next_c_url is not None:
            next_res = requests.get(next_c_url)
            next_soup = BeautifulSoup(next_res.content,"html.parser")
            statuses += _get_status(next_soup)
            next_c_url = _get_next_page_link(next_soup)

    print("Statuses Found: %d" % len(statuses))
    if len(statuses) > 0: 
        df = pd.DataFrame(statuses)
        df.columns = ['STATUS_ID', 'TWITTER_HREF', 'TIMESTAMP', 'TEXT']
        return df
    else:
        return None


if __name__ == '__main__':
    tprint("Twitter Scrapper")
    __scape_twitter_by_date()