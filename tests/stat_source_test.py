"""
Integration Test to validate the source is still returning the data in the expected format.
This seems to change year over year so the Pipeline can tell us when it changes.
"""

import pytest
from assertpy import assert_that

from services.stats import BaseService

BOXSCORE_URL = 'https://www.espn.com/nfl/boxscore/_/gameId/401547379'
TEAM_URL = 'https://www.espn.com/nfl/matchup/_/gameId/401547379'
SCHEDULE_URL = 'https://www.espn.com/nfl/schedule/_/week/2/year/2024/seasontype/2'


@pytest.fixture(scope='function')
def browser() -> BaseService:
    """
    Sets up the Browser for use in the tests.
    """

    return BaseService()


def test_get_box_score_content(browser):
    """
    Tests Retrieving the BoxScore Content from the URL.
    """

    box_score_result = browser.get_stats_payload(BOXSCORE_URL)
    assert_that(box_score_result).is_not_none()

    assert_that(box_score_result.get('page', {})
                .get('content', {})
                .get('gamepackage', {})
                .get('bxscr', {})).is_not_empty()


def test_get_team_stats_content(browser):
    """
    Tests retrieving the Team Stat Content from the URL
    """

    team_result = browser.get_stats_payload(TEAM_URL)
    assert_that(team_result).is_not_none()

    assert_that(team_result.get('page', {}).get('content', {}).get('gamepackage', {})) \
        .is_not_empty()


def test_get_schedule_content(browser):
    """
    Tests retrieving the Schedule from the URL
    """

    result = browser.get_stats_payload(SCHEDULE_URL)
    assert_that(result).is_not_none()
    assert_that(result.get('page', {}).get('content', {}).get('events', {})).is_not_empty()
