# TwitterScrapper
Python scripts to scrape and download statuses from Twitter API. Bypass 3200 statuses limit from Twitter API getting statuses from mobile web.

There are several scripts:
* twitter_scraper.py Goes to Twitter Mobile without Javascript mode and gets basic status info, and saves it in a Pandas Dataframe in MSGPack format (json binary alt) 
  * Tweet ID
  * Text
  * Date
  * Href
* twitter_api_extracion.py. Goes to Twitter API with the output of first script (DataFrame in MSGPack format) and reads all status id and get info **full-text** info from API. After extraction creates a "json" dir with all tweets in json format (one tweet for file).
  * This scripts modifies date format of twitter to get loaded in an ElasticSearch index
  * This scripts can (optional) send all tweets to an ElasticSeach index
* full_load_json_directory_es.py (Not finished yet). Loads all json files in json dir into a ElasticSearch index
* setup_elastic_es.sh. Creates a "twitter" index in ElasticSearch and put a customized mapping with **spanish analyzers** activated
  * index_status_es.json. Json with index configuration for ElasticSearch
  * mapping_status_es.json. Json with mapping configuration for ElasticSearch (spanish analyzers included)

## Enviorenment 

Scripts needs a .env file into the directory with this content

```
CONSUMER_KEY='<<YOUR_CONSUMER_KEY_FROM_TWITTER>>'
CONSUMER_SECRET='<<YOUR_CONSUMER_SECRET_FROM_TWITTER>>'
ACCESS_TOKEN_KEY='<<YOUR_ACCESS_TOKEN_KEY_FROM_TWITTER>>'
ACCESS_TOKEN_SECRET='<<YOUR_ACCESS_TOKEN_SECRET_FROM_TWITTER>>'
```
## Usage samples

Get tweets from a particular user for a month
```
python twitter_scrapper.py -q 'from:jda11on' -s 2019-01-01 -e 2019-01-31
```
More complex twitter query:
```
python twitter_scrapper.py -q 'to:ServiciosAND OR to:AndaluciaJunta OR to:OpenDataAND' -s 2011-12-01 -e 2019-03-26
```

Get Tweet data from Twitter API
```
python twitter_api_extraction.py -i 20180101_20190326--toserviciosand-or-toandaluciajunta-or-toopendataand.msg
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


