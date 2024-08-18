root_folder=$1
bucket=$2

# Games
echo "Staging Games information..."
mc cp --recursive "$root_folder/games/year=$3/type=1/"  myminio/$bucket/games/$2/preseason/
mc cp --recursive "$root_folder/games/year=$3/type=2/"  myminio/$bucket/games/$2/regular/
mc cp --recursive "$root_folder/games/year=$3/type=3/"  myminio/$bucket/games/$2/postseason/

# Teams
echo "Staging Teams Information..."
mc cp --recursive "$root_folder/teams/year=$3/type=1/"  myminio/$bucket/teams/$2/preseason/
mc cp --recursive "$root_folder/teams/year=$3/type=2/"  myminio/$bucket/teams/$2/regular/
mc cp --recursive "$root_folder/teams/year=$3/type=3/"  myminio/$bucket/teams/$2/postseason/

# Players
echo "Staging Player Information..."
mc cp --recursive "$root_folder/players/year=$3/type=1/"  myminio/$bucket/players/$2/preseason/
mc cp --recursive "$root_folder/players/year=$3/type=2/"  myminio/$bucket/players/$2/regular/
mc cp --recursive "$root_folder/players/year=$3/type=3/"  myminio/$bucket/players/$2/postseason/

echo "Done"