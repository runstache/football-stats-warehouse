#!/usr/bin/env bash

root_folder=$1
bucket=$2

types=("games" "players" "teams")

for str in "${types[@]}"; do
  echo "Staging $str information for $3..."
  mc cp --recursive "$root_folder/$str/year=$3/type=1/"  "myminio/$bucket/$str/$3/preseason/"
  mc cp --recursive "$root_folder/$str/year=$3/type=2/"  "myminio/$bucket/$str/$3/regular/"
  mc cp --recursive "$root_folder/$str/year=$3/type=3/"  "myminio/$bucket/$str/$3/postseason/"
done

echo "Done"