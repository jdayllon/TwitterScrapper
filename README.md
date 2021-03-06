# TwitterScrapper
Python scripts to scrape and download statuses from Twitter API. Bypass 3200 statuses limit from Twitter API getting statuses from mobile web.

There are several scripts:
* query_scrapper.py Goes to Twitter Mobile without Javascript mode and gets basic status info, and saves it in a Pandas Dataframe in MSGPack format (json binary alt) 
  * Tweet ID
  * Text
  * Date
  * Href
* api_extracion.py. Goes to Twitter API with the output of first script (DataFrame in MSGPack format) and reads all status id and get info **full-text** info from API. After extraction creates a "json" dir with all tweets in json format (one tweet for file).
  * This scripts modifies date format of twitter to get loaded in an ElasticSearch index
  * This scripts can (optional) send all tweets to an ElasticSeach index
* full_load_json_directory_es.py (Not finished yet). Loads all json files in json dir into a ElasticSearch index
* setup_elastic_es.sh. Creates a "twitter" index in ElasticSearch and put a customized mapping with **spanish analyzers** activated
  * index_status_es.json. Json with index configuration for ElasticSearch
  * mapping_status_es.json. Json with mapping configuration for ElasticSearch (spanish analyzers included)
* get_timeline.py. Goes to Twitter API with an user id and gets lastest statuses and loops over it up to 3200 limit. 
  * You can a start status id (since) and get all updates after that status. It can be useful to grab updates. 
* job_runner.py. Schedules querys and timelines extractions with a .yml as definition of jobs
* tools.py. Primitive common functions for scripts

## Enviorenment 

Scripts needs a .env file into the directory with this content

```
CONSUMER_KEY='<<YOUR_CONSUMER_KEY_FROM_TWITTER>>'
CONSUMER_SECRET='<<YOUR_CONSUMER_SECRET_FROM_TWITTER>>'
ACCESS_TOKEN_KEY='<<YOUR_ACCESS_TOKEN_KEY_FROM_TWITTER>>'
ACCESS_TOKEN_SECRET='<<YOUR_ACCESS_TOKEN_SECRET_FROM_TWITTER>>'
```

Job Runners uses a YML file to define jobs, this is the current format:
```
elasticsearch_uri: http://url_of_your_elasticsearch
elasticsearch_index: index_name_in_elastic_search
jobs:
  - user: some_user_twitter_id
    schedule: minutes(5)
  - query: some_twitter_query
    schedule: minutes(1)   
```

## Usage samples

Get tweets from a particular user for a month
```
python query_scrapper.py -q 'from:jda11on' -s 2019-01-01 -e 2019-01-31
```
More complex twitter query:
```
python query_scrapper.py -q 'to:ServiciosAND OR to:AndaluciaJunta OR to:OpenDataAND' -s 2011-12-01 -e 2019-03-26
```

Get Tweet data from Twitter API
```
python api_extraction.py -i 20180101_20190326--toserviciosand-or-toandaluciajunta-or-toopendataand.msg
```

Get Lastest TimeLine updates from a user
```
python get_timeline.py -u AndaluciaJunta -s 1112819068057370624 -e http://127.0.0.1:9200
```

Job Runner example:
```
python job_runner.py -j twitter_scapper.yml
```

and YML format:
```
elasticsearch_uri: http://127.0.0.1:9200
elasticsearch_index: twitter
jobs:
  - user: AndaluciaJunta
    schedule: minutes(5)
  - user: ServiciosAND
    schedule: minutes(7)
  - user: OpenDataAND
    schedule: minutes(10)
  - query: Junta de Andalucía
    schedule: minutes(10)
  - query: to:ServiciosAND OR to:AndaluciaJunta OR to:OpenDataAND
    schedule: minutes(10)
  - query: Andalucía OR Andalucia
    schedule: minutes(1)        
```

## Crontab

If you want a continous updating you can create a simple bash script to use with cron like this:
```
#!/bin/sh
output_file="$1.last_launch"
if [ -e $output_file ]
then
    value=`tail -1 $output_file`
else
    value=0
fi
python get_timeline.py -e "http://127.0.0.1:9200" -u $1 -s $value > $output_file
```

## TODOs

- [x] Add a Requierements files for pythons script or Pipenv
- [ ] Add a Docker-Compose file to create a simple ElasticSearch + Kibana service
- [x] Add config parameters to full_load_json_directory_es.py
- [x] Add comments to functions
- [ ] Add engagement info to tweet data
- [ ] Reenginering functions (maybe a "lib" script)

## References

* Elasticsearch - Defining the mapping of Twitter Data. https://medium.com/@CMpoi/elasticsearch-defining-the-mapping-of-twitter-data-dafad0f50695


