#!/bin/sh
output_file="$1.last_launch"
echo $output_file
if [ -e $output_file ]
then
    value=`tail -1 $output_file`
else
    value=0
fi
python twitter_get_3k2.py -e "http://127.0.0.1:9200" -u $1 -s $value > $output_file