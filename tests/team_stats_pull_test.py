"""
Tests for the Teams Status Data Pull
"""
import os

from assertpy import assert_that

import download_stats
from services.stats import BaseService, TeamService


def test_get_team_stats(match_up, monkeypatch):
    """
    Tests Retrieving the Team Stats
    """
    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: match_up)

    result = download_stats.get_team_stats('123456', 2024, 1, '2')
    assert_that(result).is_not_none()
    assert_that(len(result)).is_greater_than(0)


def test_get_team_stats_no_response(monkeypatch):
    """
    Tests Retrieving the Stats with no results.
    """

    monkeypatch.setattr(TeamService, 'get_team_stats', lambda *args: [])

    result = download_stats.get_team_stats('123456', 2024, 1, '2')
    assert_that(result).is_none()


def test_main(match_up, monkeypatch, schedule_frame, tmp_path):
    """
    Tests the Main Function
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: match_up)
    schedule_path = os.path.join(tmp_path.as_posix(), 'schedules', 'year=2024', 'type=2',
                                 'week_1.parquet')
    output_path = os.path.join(tmp_path.as_posix(), 'teams', 'year=2024', 'type=2')

    download_stats.main(schedule_path, output_path, 'teams')

    assert_that(os.listdir(os.path.join(tmp_path.as_posix(), 'teams', 'year=2024', 'type=2'))) \
        .contains('week_1.parquet')


def test_main_no_schedule_file(match_up, monkeypatch, tmp_path):
    """
    Tests no schedule file
    """

    output_path = os.path.join(tmp_path.as_posix(), 'teams', 'year=2024', 'type=2')
    assert_that(download_stats.main) \
        .raises(SystemExit) \
        .when_called_with('fart', output_path, 'teams')


def test_main_no_schedule_frame(monkeypatch, schedule_frame, tmp_path, caplog):
    """
    Tests no schedule frame returned
    """

    schedule_path = os.path.join(tmp_path.as_posix(), 'schedules', 'year=2024', 'type=2',
                                 'week_1.parquet')
    monkeypatch.setattr(download_stats, 'load_schedule_file', lambda *args: None)
    output_path = os.path.join(tmp_path.as_posix(), 'teams', 'year=2024', 'type=2')
    assert_that(download_stats.main) \
        .raises(SystemExit) \
        .when_called_with(schedule_path, output_path, 'teams')

    assert_that(caplog.text).contains('Schedule file is empty:')
