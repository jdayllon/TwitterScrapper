#!/usr/bin/env python
import schedule
import time
import sys
import click
from get_timeline import download_api_timeline
from query_api import query_api_statuses
import re
from loguru import logger
from settings import Settings
from tools import esK3K2_ascii_art
from huey import RedisHuey

last_status_id = {}

def user_update_job(elasticsearch_url, elasticsearch_index, screenname):
    """Executes a Job getting info from twitter user timeline
    
    Arguments:
        elasticsearch_url str -- [description]
        elasticsearch_index str -- [description]
        screenname str -- [description]
    """

    logger.info("Running 👤 : %s" % screenname)

    if screenname in last_status_id:
        last_status_id[screenname] = download_api_timeline(user=screenname, since=str(last_status_id[screenname]))
    else:
        last_status_id[screenname] = download_api_timeline(user=screenname )

    logger.info("🏁... %s -- %s" % (screenname,last_status_id[screenname]))

def query_update_job(elasticsearch_url, elasticsearch_index, query):
    """Executes a Job getting info from twitter user timeline
    
    Arguments:
        elasticsearch_url str -- [description]
        elasticsearch_index str -- [description]
        query str -- Twitter Query
    """
    #if screenname in last_status_id:
    #    last_status_id[screenname] = download_api_timeline(elasticsearch_url=elasticsearch_uri, elasticsearch_index = elasticsearch_index, user=screenname, since=str(last_status_id[screenname]))
    #else:
    #    last_status_id[screenname] = download_api_timeline(elasticsearch_url=elasticsearch_uri, elasticsearch_index = elasticsearch_index, user=screenname )
    logger.info("Running 🔎 : %s" % query)

    query_api_statuses(query, elasticsearch_url=elasticsearch_url, elasticsearch_index=elasticsearch_index )

    logger.info("🏁... 🔎 : %s" % (query))

@click.command()
@click.option('-c','--config_file', help='YML file with the definition of settings and jobs', required=True, type=str, default = 'esK3K2_defaults.yml')
def job_scheduler(config_file:str):
    """Create cron like jobs for getting info from Twitter
    
    Arguments:
        job_settings str -- YML file to define cron task
    """
    settings = Settings()._load_config(config_file)

    for cur_job in settings.jobs:

        cur_job_schedule = cur_job['schedule']
        cur_job_schedule_repeats = re.findall('\(([\d\:]*)\)',cur_job_schedule)
        cur_job_schedule_units = re.findall('([\w_]*)\([:\d]*\)',cur_job_schedule)

        if 'user' in cur_job and 'schedule' in cur_job:
        
            cur_job_user = cur_job['user']

            # If there are cur_job_schedule_repeats this means that there are a repeats in same unit different than unit
            assert len(cur_job_schedule_repeats) == 1, "YML doesnt include repeats by units on ⚙️ for 👤 [%s]" % (cur_job_user)
            assert len(cur_job_schedule_units) == 1, "YML doesnt include units on ⚙️ for 👤 [%s]" % (cur_job_user)
            cur_job_schedule_repeats_value = cur_job_schedule_repeats[0]
            
            #minutes_at(:30)
            if "at" in cur_job_schedule_units[0]:

                exec("""schedule.every().%s.at("%s").do(user_update_job, elasticsearch_url='%s', elasticsearch_index='%s', screenname="%s")""" % 
                    ( 
                        cur_job_schedule_units[0].replace("_at",""),
                        cur_job_schedule_repeats_value,
                        settings.elasticsearch_url,
                        settings.elasticsearch_status_index,
                        cur_job_user))
            else:
                exec("""schedule.every(%d).%s.do(user_update_job, elasticsearch_url='%s', elasticsearch_index='%s', screenname="%s")""" % 
                    ( 
                        int(cur_job_schedule_repeats_value),
                        cur_job_schedule_units[0],
                        settings.elasticsearch_url,
                        settings.elasticsearch_status_index,
                        cur_job_user))

            logger.info("Added ⚙️ for 👤 [%s]" % (cur_job_user))

        elif 'query' in cur_job and 'schedule' in cur_job:

            cur_job_query = cur_job['query']

            # If there are cur_job_schedule_repeats this means that there are a repeats in same unit different than unit
            assert len(cur_job_schedule_repeats) == 1, "YML doesnt include repeats by units on ⚙️ for query [%s]" % (cur_job_user)
            assert len(cur_job_schedule_units) == 1, "YML doesnt include units on ⚙️ for 🔎 [%s]" % (cur_job_query)
            cur_job_schedule_repeats_value = cur_job_schedule_repeats[0]

            if "at" in cur_job_schedule_units[0]:
                exec("""schedule.every().%s.at("%s").do(query_update_job, elasticsearch_url='%s', elasticsearch_index='%s', query="%s")""" % 
                    ( 
                        cur_job_schedule_units[0],
                        int(cur_job_schedule_repeats_value),
                        settings.elasticsearch_url,
                        settings.elasticsearch_status_index,
                        cur_job_query
                        )
                    )
            else:
                exec("""schedule.every(%d).%s.do(query_update_job, elasticsearch_url='%s', elasticsearch_index='%s', query="%s")""" % 
                    ( 
                        int(cur_job_schedule_repeats_value),
                        cur_job_schedule_units[0],
                        settings.elasticsearch_url,
                        settings.elasticsearch_status_index,
                        cur_job_query))

            logger.info("Added ⚙️ for [%s] every %s - %s " % (cur_job_query, cur_job_schedule_repeats_value, cur_job_schedule_units[0]))

        else:
            # TODO : Not implemented
            pass
    
    while True:
        schedule.run_pending()
        time.sleep(1)
    return None

#huey = RedisHuey('esK3K2', host='localhost')

if __name__ == '__main__':
    esK3K2_ascii_art()
    print("Job Runner ")

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    job_scheduler()