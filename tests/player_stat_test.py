"""
Tests for Splitting a Scrape Result into individual Stats.
"""
import json
import logging

import pytest
from assertpy import assert_that

from services.stats import PlayerService, BaseService

PAYLOAD_FILE = './tests/test_files/box-output.json'


def load_test_file(path: str) -> dict:
    """
    Loads the Test File to a Dictionary.
    :param path: Path to JSON File
    :return: Dictionary.
    """

    with open(path, 'r', encoding='utf-8-sig') as input_file:
        return json.load(input_file)


@pytest.fixture(scope='function')
def box_score() -> dict:
    """
    Box score Fixture.
    :return: Dictionary
    """

    return load_test_file(PAYLOAD_FILE)


@pytest.fixture(autouse=True)
def setup_logging():
    """
    Sets up the Basic Logging
    """
    logging.basicConfig(level=logging.INFO)


def test_get_player_stats(monkeypatch, box_score):
    """
    Tests Retrieving the Player Stats and creating the objects.
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: box_score)
    service = PlayerService()

    result = service.get_player_stats('99999', 1, 2023, 'regular')

    # Test all stats have the same week, year and game type
    assert_that([x for x in result if x.get('week', 0) == 1
                 and x.get('year', 0) == 2023
                 and x.get('game_type', '') == 'regular']).is_not_empty().is_length(len(result))

    # Test the general field mapping
    assert_that([x for x in result if x.get('player_name', '') == 'Patrick Mahomes'
                 and x.get('player_url',
                           '') == 'http://www.espn.com/nfl/player/_/id/3139477/patrick-mahomes'
                 and x.get('statistic_value', 0.0) == 241.0
                 and x.get('statistic_name', '') == 'passingyards'
                 and x.get('statistic_type', '') == 'passing'
                 and x.get('statistic_code', '') == 'yds'
                 and x.get('team', '') == 'Kansas City Chiefs'
                 and x.get('opponent' '') == 'Baltimore Ravens']).is_not_empty()

    assert_that([x for x in result if x.get('player_name', '') == 'Lamar Jackson'
                 and x.get('player_url',
                           '') == 'http://www.espn.com/nfl/player/_/id/3916387/lamar-jackson'
                 and x.get('statistic_value', 0.0) == 272.0
                 and x.get('statistic_name', '') == 'passingyards'
                 and x.get('statistic_type', '') == 'passing'
                 and x.get('statistic_code', '') == 'yds'
                 and x.get('team', '') == 'Baltimore Ravens'
                 and x.get('opponent' '') == 'Kansas City Chiefs']).is_not_empty()


def test_split_stats():
    """
    Tests splitting a stat value.
    """

    stat = {
        'player_url': 'http://www.espn.com/nfl/player/_/id/3139477/patrick-mahomes',
        'player_name': 'Patrick Mahomes',
        'team': 'Kansas City Chiefs',
        'opponent': 'Baltimore Ravens'
    }

    service = PlayerService()

    result = service._split_stat_(stat, '30/39', 'C/ATT', 'completions/passingAttempts')
    assert_that(result).is_not_empty().is_length(2)
    assert_that([x for x in result if x.get('statistic_name', '') == 'completions'
                 and x.get('statistic_code', '') == 'c'
                 and x.get('statistic_value', 0) == 30.0]).is_not_empty()

    assert_that([x for x in result if x.get('statistic_name', '') == 'passingattempts'
                 and x.get('statistic_code', '') == 'att'
                 and x.get('statistic_value', 0) == 39.0]).is_not_empty()


def test_split_stats_non_numeric():
    """
    Tests Splitting a Stat that is not numeric
    """

    stat = {
        'player_url': 'http://www.espn.com/nfl/player/_/id/3139477/patrick-mahomes',
        'player_name': 'Patrick Mahomes',
        'team': 'Kansas City Chiefs',
        'opponent': 'Baltimore Ravens'
    }

    service = PlayerService()

    result = service._split_stat_(stat, '30/A', 'C/ATT', 'completions/passingAttempts')
    assert_that(result).is_not_empty().is_length(2)
    assert_that([x for x in result if x.get('statistic_name', '') == 'completions'
                 and x.get('statistic_code', '') == 'c'
                 and x.get('statistic_value', 0) == 30.0]).is_not_empty()

    assert_that([x for x in result if x.get('statistic_name', '') == 'passingattempts'
                 and x.get('statistic_code', '') == 'att'
                 and x.get('statistic_value', '') == 0.0]).is_not_empty()


def test_no_response_payload(monkeypatch):
    """
    Tests no response payload returns.
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: {})
    service = PlayerService()

    result = service.get_player_stats('99999', 1, 2023, 'regular')
    assert_that(result).is_empty()


def test_split_stat_different_lengths():
    """
    Tests the Split Stat Function with different lengths of splits.
    """

    code = 'PA/PC'
    label = 'PassingAttempt/PassingComplete'
    value = '20/30/40'

    stat = {}

    service = PlayerService()

    result = service._split_stat_(stat, value, code, label)

    assert_that(result).is_empty()


def test_build_stats_category_key_mismatch(caplog):
    """
    Tests Building Stats Category where the Keys and value counts are mismatched.
    """

    stat = {
        'keys': ['st1', 'st2'],
        'lbls': ['lbl1'],
        'athlts': [],
        'type': ''

    }

    service = PlayerService()
    result = service._build_stats_category_([stat], 'team1', 'team2')
    assert_that(result).is_empty()

    assert_that(caplog.text).contains('Labels and Key List not the same size for')


def test_build_athlete_stat_more_values_than_categories():
    """
    Tests a mismatch in codes and labels returns an empty set.
    """

    athlete = {
        'athlt': {
            'dspNm': 'Jeff',
            'lnk': 'https://localhost/player1'
        },
        'stats': [
            {
                'n': 'XP',
                'd': '1/2'
            },
            {
                'n': 'PA',
                'd': '1/2'
            }
        ]
    }

    keys = ['k1']
    labels = ['l1']
    stat_type = 'passing'

    service = PlayerService()
    results = service._build_athlete_stats_(athlete, keys, labels, stat_type)

    assert_that(results).is_length(1)


def test_empty_boxs_core(caplog):
    """
    Tests an Empty Box Score
    """

    boxscore = {
        'page': {
            'content': {
                'bxscr': []
            }
        }
    }

    service = PlayerService()
    result = service._build_stats_(boxscore)

    assert_that(result).is_empty()
    assert_that(caplog.text).contains('Home and Away Team Stats not specified.')


def test_two_entry_box_score(caplog):
    """
    Tests a Box score with two entries usually meaning an empty one.
    """

    boxscore = {
        'page': {
            'content': {
                'bxscr': [{
                    'team': 't1'
                },
                    {
                        'team': 't2'
                    }]
            }
        }
    }

    service = PlayerService()
    result = service._build_stats_(boxscore)

    assert_that(result).is_empty()
    assert_that(caplog.text).contains('Home and Away Team Stats not specified.')
