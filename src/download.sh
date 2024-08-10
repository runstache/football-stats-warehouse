scheduled_folder=$1
search_directory="$scheduled_folder/year=$2/type=$3"
for entry in "$search_directory"/*
do
  echo $entry
done
