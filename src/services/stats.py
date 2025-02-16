"""
Services for working with Stats retrieval.
"""

import logging
import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service


class BaseService:
    """
    Base Service Class
    """
    browser: webdriver.Chrome
    logger: logging.Logger

    def __init__(self) -> None:
        """
        Base Service Constructor to establish a Web Browser.
        """
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--ignore-certificate-errors')

        if os.getenv('SELENIUM_DRIVER'):
            service = Service(os.getenv('SELENIUM_DRIVER'))
            self.browser = webdriver.Chrome(options=options, service=service)
        else:
            self.browser = webdriver.Chrome(options=options)
        self.logger = logging.getLogger(__name__)

    def get_stats_payload(self, url: str) -> dict | None:
        """
        Retrieves the Stats Payload from the Provided URL.
        :param url: URL to request.
        :return: Dictionary or None.
        """
        self.browser.get(url)
        return self.browser.execute_script('return window.__espnfitt__')

    def __del__(self):
        """
        Destructor for Closing up the Selenium Web Browser.
        """
        self.browser.quit()


class TeamService(BaseService):
    """
    Service for Retrieving and Processing Team Level Statistics.
    """

    @staticmethod
    def _split_stat_(value: str) -> list[float]:
        """
        Splits a String value into a List of Floats
        :param value: Value to split
        :return: List of Floats
        """

        if ':' not in value and '-' not in value:
            if value.isnumeric():
                return [float(value)]
            return []

        delimiter = ''

        if ':' in value:
            delimiter = ':'

        if '-' in value:
            delimiter = '-'

        parts = value.split(delimiter)
        return [float(x) for x in parts if x.isnumeric()]

    def _create_split_stats_(self, team: dict, opponent: str,
                             stat_entry: dict, mappings: dict) -> list[dict]:

        """
        Creates a set of split stats
        :param team: Team Info
        :param opponent: Opponent
        :param stat_entry: Stats Entry Value
        :param mappings: Key Mapping
        :return: List if Dictionaries
        """

        result = {
            'team': team.get('team', ''),
            'team_url': team.get('url', ''),
            'opponent': opponent
        }
        mapping = mappings.get(stat_entry.get('n', ''))
        if not mapping:
            return []

        values = stat_entry.get('d', '')
        parts = self._split_stat_(values)
        if len(parts) == 2:
            result['statistic_name'] = mapping[0]
            result['statistic_value'] = parts[0]

            attempts = result.copy()
            attempts['statistic_name'] = mapping[1]
            attempts['statistic_value'] = parts[1]

            return [result, attempts]
        return []

    def _create_stats_(self, team: dict, opponent: str, stats: dict) -> list[dict]:
        """
        Parses the Statistics into a listing of stats.
        :param team: Team Name
        :param opponent: Opponents
        :param stats: Statistic values
        :return: List of Dictionaries
        """

        results = []
        mappings = {
            'completionAttempts': ['completions', 'attempts'],
            'fourthDownEff': ['fourthdowncompletions', 'fourthdownattempts'],
            'redZoneAttempts': ['redzonecompletions', 'redzoneattempts'],
            'sacksYardsLost': ['sacks', 'sackyards'],
            'thirdDownEff': ['thirddowncompletions', 'thirddownattempts'],
            'totalPenaltiesYards': ['penalties', 'penaltyyards'],
        }

        for item in stats.items():
            values = dict(item[1])
            if item[0] in mappings:
                split_results = self._create_split_stats_(team, opponent, values, mappings)
                if split_results:
                    results.extend(split_results)
                continue

            result = {
                'team': team.get('team', ''),
                'team_url': team.get('url', ''),
                'opponent': opponent
            }

            if item[0] == 'possessionTime':
                parts = self._split_stat_(values.get('d', ''))
                if len(parts) == 2:
                    total_time = (parts[0] * 60) + parts[1]
                    result['statistic_name'] = 'possessiontime'
                    result['statistic_value'] = total_time
                continue

            result['statistic_name'] = values.get('n', '').lower()
            result['statistic_value'] = float(values.get('d', 0.0))
            results.append(result)
        return results

    def get_team_stats(self, game_id: str, week: int, year: int, game_type: str) -> list[dict]:
        """
        Retrieves the Team Level Statistics from a game.
        :param game_id: Unique Game ID
        :param week: Week Number
        :param year: Year Value
        :param game_type: Game Type
        :return: List of Dictionaries
        """

        def add_partitions(item: dict, week_value: int, year_value: int, gtype: str) -> dict:
            """
            Generator Function that Adds the Week, Year and Type to each item
            :param item: List Entry
            :param week_value: Week Number
            :param year_value: Year Number
            :param gtype: Game Type
            :return: Item
            """
            item['week'] = week_value
            item['year'] = year_value
            item['game_type'] = gtype
            return item

        url = f"https://www.espn.com/nfl/matchup/_/gameId/{game_id}"
        payload = self.get_stats_payload(url)
        results = []

        if not payload:
            self.logger.warning('No Stats returned for %s', game_id)
            return []

        home_team_entry = payload.get('page', {}).get('content', {}).get('gamepackage', {}).get(
            'tmStats', {}).get('home', {})
        away_team_entry = payload.get('page', {}).get('content', {}).get('gamepackage', {}).get(
            'tmStats', {}).get('away', {})

        home_team = {
            'team': home_team_entry.get('t', {}).get('dspNm', ''),
            'url': 'https://www.espn.com' + home_team_entry.get('t', {}).get('lnk', '')
        }

        away_team = {
            'team': away_team_entry.get('t', {}).get('dspNm', ''),
            'url': 'https://www.espn.com' + away_team_entry.get('t', {}).get('lnk', '')
        }

        home_stats = self._create_stats_(home_team, away_team.get('team', ''),
                                         home_team_entry.get('s', {}))
        away_stats = self._create_stats_(away_team, home_team.get('team', ''),
                                         away_team_entry.get('s', {}))

        if home_stats:
            results.extend(home_stats)
        if away_stats:
            results.extend(away_stats)

        return list(add_partitions(x, week, year, game_type) for x in results)


class ScheduleService(BaseService):
    """
    Service for retrieving Schedule Information
    """

    @staticmethod
    def _process_events_(events: list[dict]) -> list[dict]:
        """
        Parses the Event details from a list of events.
        :param events: List of Events
        :return: List of Dictionaries
        """

        items = []

        for event in events:
            home_team = [x for x in event.get('competitors', []) if x.get('isHome', False) is True][
                0]
            away_team = \
                [x for x in event.get('competitors', []) if x.get('isHome', False) is False][0]

            items.append({
                'game_id': event.get('id', ''),
                'home_team_code': home_team.get('abbrev', ''),
                'home_team': home_team.get('displayName', ''),
                'away_team_code': away_team.get('abbrev', ''),
                'away_team': away_team.get('displayName', '')
            })

        return items

    def get_schedule(self, week: int, year: int, game_type: int) -> list[dict]:
        """
        Retrieves a listing of Game IDs for the provided Week, Year and Game Type.
        :param week: Week Number
        :param year: Season Year
        :param game_type: Game Type ID value (1,2,3)
        :return: List of Game Information
        """

        def add_common_fields(row: dict, year_value: int, week_value: int, type_id: int,
                              game_date: str) -> dict:
            """
            Generator Function to add the common fields to the row.
            :param row: Row to update
            :param year_value: Year Value
            :param week_value: Week Value
            :param type_id: Game Type
            :param game_date: Date Value
            :return: Updated Row
            """

            update_row = row.copy()
            update_row['year'] = year_value
            update_row['week'] = week_value
            update_row['game_type'] = type_id
            update_row['game_date'] = game_date
            return update_row

        url = f"https://www.espn.com/nfl/schedule/_/week/{week}/year/{year}/seasontype/{game_type}"
        response = self.get_stats_payload(url)
        if not response:
            return []

        events = response.get('page', {}).get('content', {}).get('events', {})
        results = []
        for item in events.items():
            date_value = item[0]
            items = item[1]
            games = self._process_events_(items)

            results.extend([add_common_fields(x, year, week, game_type, date_value) for x in games])
        return results


class GameService(BaseService):
    """
    Service for retrieving Game Information.
    """

    def get_game_info(self, game_id: str, week: int, year: int, game_type: str) -> dict | None:
        """
        Returns the Information concerning the game played.
        :param game_id: Game ID
        :param week: Week Number
        :param year: Season Year
        :param game_type: Game Type (Preseason, Regular, Postseason)
        :return: Dictionary
        """

        url = f"https://www.espn.com/nfl/matchup/_/gameId/{game_id}"
        stats_payload = self.get_stats_payload(url)

        if not stats_payload:
            return None

        game_info = stats_payload.get('page', {}).get('content', {}).get('gamepackage', {}).get(
            'gmInfo', {})
        team_stats = stats_payload.get('page', {}).get('content', {}).get('gamepackage', {}).get(
            'tmStats', {})
        game_strip = stats_payload.get('page', {}).get('content', {}).get('gamepackage', {}).get(
            'gmStrp', {})

        home_team = team_stats.get('home', {}).get('t', {})
        away_team = team_stats.get('away', {}).get('t', {})

        return {
            'game_id': game_id,
            'home_team': home_team.get('dspNm', ''),
            'away_team': away_team.get('dspNm', ''),
            'location': game_info.get('loc', ''),
            'city': game_info.get('locAddr', {}).get('city', ''),
            'state': game_info.get('locAddr', {}).get('state', ''),
            'game_date': game_info.get('dtTm', {}),
            'week': week,
            'year': year,
            'game_type': game_type,
            'is_conference': game_strip.get('isConferenceGame', False),
            'note': game_strip.get('nte', ''),
            'home_score': [int(x.get('score', 0)) for x in game_strip.get('tms', []) if
                           x.get('isHome', False) is True][0],
            'away_score': [int(x.get('score', 0)) for x in game_strip.get('tms', []) if
                           x.get('isHome', False) is False][0],
            'line': game_info.get('lne', '').split(' ')[-1],
            'over_under': game_info.get('ovUnd', 0)
        }


class PlayerService(BaseService):
    """
    Service for Retrieving and Processing Player Level Stats.
    """

    def get_player_stats(self, game_id: str, week: int, year: int, game_type: str) -> list[dict]:
        """
        Retrieves the Player Stats from the provided Game ID.
        :param game_id: Game ID
        :param week: Week Number
        :param year: Season Year
        :param game_type: Type of Game (Pre, Regular, Post
        :return: List of Dictionaries
        """

        def add_partitions(item: dict, week_value: int, year_value: int, gtype: str) -> dict:
            """
            Generator Function that Adds the Week, Year and Type to each item
            :param item: List Entry
            :param week_value: Week Number
            :param year_value: Year Number
            :param gtype: Game Type
            :return: Item
            """
            item['week'] = week_value
            item['year'] = year_value
            item['game_type'] = gtype
            return item

        url = f"https://www.espn.com/nfl/boxscore/_/gameId/{game_id}"

        payload = self.get_stats_payload(url)
        if not payload:
            self.logger.warning('No Stats returned for %s', game_id)
            return []

        results = self._build_stats_(payload)
        return list(add_partitions(x, week, year, game_type) for x in results)

    def _build_stats_(self, box_score: dict) -> list[dict]:
        """
        Creates the Listing of Stats from the Box Score.
        :param box_score: Box Score
        :return: List of scores.
        """
        records = []

        box_score_stats = box_score.get('page', {}).get('content', {}).get('gamepackage', {}).get(
            'bxscr', [])
        if not box_score_stats and len(box_score_stats) != 2:
            self.logger.warning('Home and Away Team Stats not specified.')
            return []

        away_team_entry: dict = \
            [x for x in box_score_stats if x.get('tm', {}).get('hm', False) is False][0]
        home_team_entry: dict = \
            [x for x in box_score_stats if x.get('tm', {}).get('hm', False) is True][0]

        home_team = home_team_entry.get('tm', {}).get('dspNm', '')
        away_team = away_team_entry.get('tm', {}).get('dspNm', '')

        home_team_stats = home_team_entry.get('stats', [])
        away_team_stats = away_team_entry.get('stats', [])

        records.extend(self._build_stats_category_(home_team_stats, home_team, away_team))
        records.extend(self._build_stats_category_(away_team_stats, away_team, home_team))

        return records

    def _build_stats_category_(self, stats: list[dict], team: str, opponent: str) -> list[dict]:
        """
        Creates the category of Statistics Entries for a player.
        :param stats: Stats to Process.
        :param team: Team Name
        :param opponent: Opponent Team Name
        :return: List of Dictionaries
        """

        def add_teams(item: dict, team_value: str, opponent_value: str) -> dict:
            """
            Generator Function for adding the Team and Opponent to the Stats.
            :param item: Stat Item
            :param team_value: Team
            :param opponent_value: Opponent
            :return: None
            """

            item['team'] = team_value
            item['opponent'] = opponent_value
            return item

        records = []
        for stat_category in stats:
            keys = stat_category.get('keys', [])
            labels = stat_category.get('lbls', [])
            stat_type = stat_category.get('type', '')
            athletes = stat_category.get('athlts', [])

            it = iter([keys, labels])
            length_check = len(next(it))
            if not all(len(x) == length_check for x in it):
                self.logger.warning('Labels and Key List not the same size for %s', stat_type)
            for athlete in athletes:
                results = self._build_athlete_stats_(athlete, keys, labels, stat_type)
                records.extend(results)
        if records:
            return list(add_teams(x, team, opponent) for x in records)
        return []

    def _build_athlete_stats_(self, athlete: dict, keys: list[str], labels: list[str],
                              stat_type: str) -> list[dict]:
        """
        Creates Stats Entries for a given Athlete.
        :param athlete: Athlete Stats Entry
        :param keys: Statistic Codes
        :param labels: Statistic Labels
        :param stat_type: Statistic Type
        :return: List of Statistics
        """

        def add_stat_type(item: dict, stype: str) -> dict:
            """
            Generator Function for Adding the Statistic Type.
            :param item: Item to Add the Value.
            :param stype: Value to Add
            :return: None
            """
            item['statistic_type'] = stype
            return item

        records = []
        player = {
            'player_name': athlete.get('athlt', {}).get('dspNm', ''),
            'player_url': athlete.get('athlt', {}).get('lnk', '')
        }

        values: list[str] = athlete.get('stats', [])
        index = 0
        for value in values:

            if index > len(labels) - 1 or index > len(keys) - 1:
                continue

            player_stat = player.copy()
            if '/' in value:
                records.extend(self._split_stat_(player_stat, value, labels[index], keys[index]))
            else:
                player_stat['statistic_code'] = labels[index].lower()
                player_stat['statistic_name'] = keys[index].lower()
                player_stat['statistic_value'] = float(value) if str(value).isnumeric() else 0.0

                records.append(player_stat)
            index += 1
        return list(add_stat_type(x, stat_type) for x in records)

    def _split_stat_(self, stat: dict, value: str, code: str, label: str) -> list[dict]:
        """
        Splits a Statistic into two Stats when there is a separator.
        :param stat: Stat Object
        :param value: Stat Value
        :param code: Stat Code
        :param label: Stat Label
        :return: Collection of Stats
        """

        if code == 'XP':
            codes = ['XPM', 'XPA']
        elif code == 'FG':
            codes = ['FGM', 'FGA']
        else:
            codes = code.split('/')
        labels = label.split('/')
        values = value.split('/')

        it = iter([codes, labels, values])
        length_check = len(next(it))

        if not all(len(x) == length_check for x in it):
            self.logger.warning(
                'All lists are not the same size for splitting the stat: %s | %s | %s', code, label,
                value)
            return []

        stats = []

        index = 0
        for stat_value in values:
            player_stat = stat.copy()
            player_stat['statistic_code'] = codes[index].lower()
            player_stat['statistic_name'] = labels[index].lower()

            if stat_value.isnumeric():
                player_stat['statistic_value'] = float(stat_value)
            else:
                player_stat['statistic_value'] = 0.0

            stats.append(player_stat)
            index += 1

        return stats
