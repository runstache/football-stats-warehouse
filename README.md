# Football Stats Warehouse

Project for creating a Columnar Data warehouse for Football Stats

## Entity Structure

The warehouse comprises the following Entities:

* Player Statistics - Statistics from a game attributed to a specific player
* Team Statistics - Statistics from a game attributed to the entire team
* Games - Categorical Information concerning the Game played

### Player Statistics

The Player Statistics Entity will contain the following properties

* Game Id - The Unique ID for the given Game.
* Url - URL to player information.
* Player Name - The name of the player
* Statistic Code - The Coded Value for the Statistic
* Statistic Name - The Display name of the statistic.
* Statistic Type - The Type of Statistic (passing, rushing, defense, etc)
* Statistic Value - Value of the Statistic
* Team - The Team the player is representing
* Opponent - The Opponent Team
* Week - The Season Week
* Year - The Season year
* Game Type - Denotes Pre, Post, Regular Season

### Team Statistics

The Team Statistics Entity will contain the following properties:

* Game Id - The Unique ID for the Given Game
* Team Name - The Team Name
* Statistic Code - The Coded Value for the Statistic
* Statistic Name - The Display Name of the Statistic
* Statistic Value - The Value of the Statistic captured
* Opponent - The Opponent Team
* Week - The Season Week
* Year - The Season year
* Game Type - Denotes Pre, Post, Regular Season

### Game Information

The Game Information Entity will contain the following properties:

* Game Id - The Unique Game ID
* Game Date - The Date/Time the Game was played
* Location - Geographic Location of the Game
* Venue - The Venue the game was held
* Home Team - Home Team Name
* Away Team - Away Team Name
* Home Team Score - Final score for the Home Team
* Away Team Score - Final score for the Away Team
* Total Score - Total Points Scored
* Betting Line - Provided Betting Line
* Over Under - Total Points Over/Under
* Attendance - Total Attendance for the Game
* Week - The Season Week
* Year - The Season year
* Game Type - Denotes Pre, Post, Regular Season

### Schedule Item

The Schedule Information Entity will contain the following properties:

* Game Id: The Game ID - Game ID Value
* Home Team Code - Home Team Code
* Home Team - Home Team Name
* Away Team Code - Away Team Code
* Away Team - Away Team Name
* Week - Season Week
* Year - Season Year
* Game Type: Type of Game (1,2,3)
* Game Date: Date of the game (%Y%m%d)

## Formats

The scripts in this repository generate the Entities contained in Parquet Files. These files are then stored in a file-based partition
schema in the following format:

/year/season type/week/teams/GameId.parquet
/year/season type/week/players/GameId.parquet
/year/season type/games/week-number.parquet

Each Entity contains properties for these partitioning schemes as well to allow for other consumption

## Services

The majority of the extraction of the data is being accomplished through each of the services. The following services are available:

* Player Service - Scrapes the Player Statistics from the source. All Categories are extracted for both teams and the categories.
* Team Service - Scrapes the Team Statistics from the source. Both Teams are captured.
* Game Service - Retrieves the Game Information from the source.
* Schedule Service - Retrieves the Games available for a given Year, Week and Game Type

### Player Service

The Player Service contains one public method, __get_player_stats__. This method retrieves the stats based on the following parameters:

* game_id - Unique Game Id 
* week - Week Number
* year - Year Value
* game_type - Type of Game (Preseason, Regular, Postseason)

The return is a list of dictionary objects containing the following information:

* Player Name (player_name): Name of the Player
* Player Url (player_url): Unique URL for the player
* Statistic Name (statistic_name): Name of th Statistic
* Statistic Type (statistic_type): Category of Statistic. (Passing, Rushing, etc.)
* Statistic Code (statistic_code): Coded value from the data representing the Statistic
* Team (team): Player Team
* Opponent (opponent): Game Opponent
* Statistic Value (statistic_value): Float Value of the Statistic

```python

from services.stats import PlayerService

service = PlayerService()

results = service.get_player_stats('12345', 1, 2023, 'regular')

```

### Team Service

The Team service contains one public method, __get_team_stats__. This method retrieves the team level stats based on the following parameters.

* game_id - Unique Game ID
* week - Week Number
* year - Year Value
* game_type - Type of Game (Preseason, Regular, Postseason)

The return is a list of dictionaries containing the folling information:

* Team Name (team):
* Team Url (team_url):
* Opponent (opponent):
* Statistic Name (statistic_name): Name of th Statistic
* Statistic Value (statistic_value): Float Value of the Statistic
* Week (week): Week of the Schedule
* Year (year): Season Year
* Game Type (game_type): Type of Game (Preseason, Regular, Postseason)

```python

from services.stats import TeamService

service = TeamService()

result = service.get_team_stats('123456', 1, 2023, 'regular')

```

### Schedule Service

The Schedule Service contains one public method, __get_schedule__. This method is used for retrieving the schedule information for the following
parameters:

* week: Week Number
* year: Season Year
* game_type: Type of Game (Preseason[1], Regular[2], Postseason[3])

The return is a list of Events that are occurring on the Week and Year provided.

```python

from services.stats import ScheduleService

service = ScheduleService()

result = service.get_schedule(1, 2023, 2)

```

### Game Service

The Game Service contains one public method, __get_game_info__. This method is used for retrieving information about a specific game through the
following parameters:

* Game ID (game_id): The Unique Game ID
* Week (week): Week number of the season
* Year (year): Year value of the season
* Game Type (game_type): Type of Game (Preseason, Regular, Postseason)

The method returns a dictionary with the following information:

* Game ID (game_id): Unique Game ID
* Home Team (home_team): Home Team Name
* Away Team (away_team): Away Team
* Location (location): Stadium Location
* City (city): City the game was played in
* State (state): State the game was played in
* Game Date (game_date): Date/Time of the Game
* Week Number (week): Season Week
* Year (year): Season Year
* Game Type (game_type): Type of Game (Preseason, Regular, Postseason)
* Conference Game (is_conference): Denotes the game is a conference game
* Game Note (note): Additional Notes on the game
* Home Team Score (home_score): Total Points Scored by the Home Team 
* Away Team Score (away_score): Total Points Scored by the Away Team
* Betting Line (line): Betting Line
* Over/Under (over_under): Over/Under Total Points

```python
from services.stats import GameService


service = GameService()
results = service.get_game_info('12345')

```

## Container Utilization

The application is containerized to allow for executing the contained scripts via K8 Jobs. These scripts leverage the following Environment Variables
to provide access to S3 buckets and the scraping throught Selenium:

* AWS_ACCESS_KEY_ID: AWS Access Key
* AWS_SECRET_ACCESS_KEY: AWS Secret Access Key
* S3_ENDPOINT: Override for S3 URL to allow for use of Minio
* SELENIUM_DRIVER: Path to the Chrom Web Driver (/usr/bin/webdriver)