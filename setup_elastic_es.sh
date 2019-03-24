#!/bin/bash
# Index Creation with spanish custom analyzer
echo -e "\n Index Deletion \n"
curl -X DELETE "localhost:9200/twitter" -H 'Content-Type: application/json'
echo -e "\n Index Creation \n"
curl -X PUT "localhost:9200/twitter" -H 'Content-Type: application/json' -d '@index_status_es.json'
echo -e "\n Mapping Creation \n"
# Mapping Creation with spanish custom analyzer
curl -X PUT "localhost:9200/twitter/_mapping/status?include_type_name=true" -H 'Content-Type: application/json' -d @mapping_status_es.json