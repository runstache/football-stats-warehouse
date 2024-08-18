#!/usr/bin/env bash

root_folder=$1
scheduled_folder=$root_folder/schedules
search_directory="$scheduled_folder/year=$2/type=$3"
for entry in "$search_directory"/*
do
  echo Processing $entry
  python ./download_stats.py -s $entry -o $root_folder/$4/year=$2/type=$3 -t $4
done
