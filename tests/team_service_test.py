"""
Tests for the Team Service.
"""
import json
import logging

import pytest
from assertpy import assert_that

from services.stats import TeamService, BaseService

RESULT_FILE = './tests/test_files/team-output.json'


@pytest.fixture
def match_up() -> dict:
    """
    Loads the Team Match up data
    :return: Dictionary
    """

    with open(RESULT_FILE, 'r', encoding='utf-8-sig') as input_file:
        return json.load(input_file)


@pytest.fixture(autouse=True)
def setup_logging():
    """
    Sets up the logging
    """
    logging.basicConfig(level=logging.INFO)


def test_get_team_stats(monkeypatch, match_up):
    """
    Tests Retrieving the Team Match Up Stats.
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: match_up)

    service = TeamService()

    result = service.get_team_stats('12345', 3, 2023, 'regular')

    assert_that([x for x in result if x.get('team', '') == 'Kansas City Chiefs'
                 and x.get('team_url',
                           '') == 'https://www.espn.com/nfl/team/_/name/kc/kansas-city-chiefs'
                 and x.get('opponent', '') == 'Baltimore Ravens'
                 and x.get('statistic_name', '') == 'firstdowns'
                 and x.get('statistic_value', '') == 22.0]).is_not_empty()

    assert_that([x for x in result if x.get('team', '') == 'Baltimore Ravens'
                 and x.get('team_url',
                           '') == 'https://www.espn.com/nfl/team/_/name/bal/baltimore-ravens'
                 and x.get('opponent', '') == 'Kansas City Chiefs'
                 and x.get('statistic_name', '') == 'firstdowns'
                 and x.get('statistic_value', '') == 16.0]).is_not_empty()


def test_get_team_stats_split_value(monkeypatch, match_up):
    """
    Tests Retrieving the Team Match up with a Split Stat.
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: match_up)

    service = TeamService()

    result = service.get_team_stats('12345', 3, 2023, 'regular')

    assert_that([x for x in result if x.get('team', '') == 'Kansas City Chiefs'
                 and x.get('team_url',
                           '') == 'https://www.espn.com/nfl/team/_/name/kc/kansas-city-chiefs'
                 and x.get('opponent', '') == 'Baltimore Ravens'
                 and x.get('statistic_name', '') == 'completions'
                 and x.get('statistic_value', '') == 30.0]).is_not_empty()

    assert_that([x for x in result if x.get('team', '') == 'Kansas City Chiefs'
                 and x.get('team_url',
                           '') == 'https://www.espn.com/nfl/team/_/name/kc/kansas-city-chiefs'
                 and x.get('opponent', '') == 'Baltimore Ravens'
                 and x.get('statistic_name', '') == 'attempts'
                 and x.get('statistic_value', '') == 39.0]).is_not_empty()

    assert_that([x for x in result if x.get('team', '') == 'Baltimore Ravens'
                 and x.get('team_url',
                           '') == 'https://www.espn.com/nfl/team/_/name/bal/baltimore-ravens'
                 and x.get('opponent', '') == 'Kansas City Chiefs'
                 and x.get('statistic_name', '') == 'completions'
                 and x.get('statistic_value', '') == 20.0]).is_not_empty()

    assert_that([x for x in result if x.get('team', '') == 'Baltimore Ravens'
                 and x.get('team_url',
                           '') == 'https://www.espn.com/nfl/team/_/name/bal/baltimore-ravens'
                 and x.get('opponent', '') == 'Kansas City Chiefs'
                 and x.get('statistic_name', '') == 'attempts'
                 and x.get('statistic_value', '') == 37.0]).is_not_empty()


def test_split_values():
    """
    Tests splitting a value.
    """

    result = TeamService._split_stat_('20-30')
    assert_that(result).contains_only(20.0, 30.0)


def test_split_value_no_separator():
    """
    Tests splitting a value without a separator.
    """

    result = TeamService._split_stat_('30')
    assert_that(result).contains_only(30.0)


def test_split_values_colon():
    """
    Tests splitting a value with a colon separator
    """

    result = TeamService._split_stat_('30:45')
    assert_that(result).contains_only(30.0, 45.0)


def test_split_values_non_numeric():
    """
    Tests splitting a value with a non-numeric value
    """

    result = TeamService._split_stat_('ABC')
    assert_that(result).is_empty()


def test_team_service_partitions(monkeypatch, match_up):
    """
    Tests the Partitions are present on all records.
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: match_up)

    service = TeamService()

    result = service.get_team_stats('12345', 3, 2023, 'regular')

    # Test all stats have the same week, year and game type
    assert_that([x for x in result if x.get('week', 0) == 3
                 and x.get('year', 0) == 2023
                 and x.get('game_type', '') == 'regular']).is_not_empty().is_length(len(result))


def test_team_service_no_payload(monkeypatch):
    """
    Tests no Response Payload returned.
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: {})

    service = TeamService()

    result = service.get_team_stats('12345', 3, 2023, 'regular')
    assert_that(result).is_empty()


def test_team_create_split_stats():
    """
    Tests Creating split stats
    """

    team = {
        'team_name': 'Boston',
        'team_url': 'https://localhost/boston'
    }

    mappings = {
        'completionAttempts': ['completions', 'attempts'],
        'fourthDownEff': ['fourthdowncompletions', 'fourthdownattempts'],
        'redZoneAttempts': ['redzonecompletions', 'redzoneattempts'],
        'sacksYardsLost': ['sacks', 'sackyards'],
        'thirdDownEff': ['thirddowncompletions', 'thirddownattempts'],
        'totalPenaltiesYards': ['penalties', 'penaltyyards'],
    }

    stat_entry = {
        'n': 'completionAttempts',
        'd': '20-30'
    }

    service = TeamService()
    result = service._create_split_stats_(team, 'Buffalo', stat_entry, mappings)
    assert_that(result).extracting('statistic_name', 'statistic_value').contains(
        ('completions', 20), ('attempts', 30))

def test_create_split_stat_no_mapping():
    """
    Tests creating Split Stat with No Mappings
    """

    team = {
        'team_name': 'Boston',
        'team_url': 'https://localhost/boston'
    }

    mappings = {
    }

    stat_entry = {
        'n': 'completionAttempts',
        'd': '20-30'
    }

    service = TeamService()
    result = service._create_split_stats_(team, 'Buffalo', stat_entry, mappings)
    assert_that(result).is_empty()

def test_create_split_stat_too_many_splits():
    """
    Tests too many splits
    """

    team = {
        'team_name': 'Boston',
        'team_url': 'https://localhost/boston'
    }

    mappings = {
        'completionAttempts': ['completions', 'attempts'],
        'fourthDownEff': ['fourthdowncompletions', 'fourthdownattempts'],
        'redZoneAttempts': ['redzonecompletions', 'redzoneattempts'],
        'sacksYardsLost': ['sacks', 'sackyards'],
        'thirdDownEff': ['thirddowncompletions', 'thirddownattempts'],
        'totalPenaltiesYards': ['penalties', 'penaltyyards'],
    }

    stat_entry = {
        'n': 'completionAttempts',
        'd': '20-30-50'
    }

    service = TeamService()
    result = service._create_split_stats_(team, 'Buffalo', stat_entry, mappings)
    assert_that(result).is_empty()