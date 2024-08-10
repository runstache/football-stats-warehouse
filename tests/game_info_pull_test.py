"""
Tests for the Teams Status Data Pull
"""
import os

from assertpy import assert_that

import download_stats
from services.stats import BaseService, GameService


def test_get_game_info(match_up, monkeypatch):
    """
    Tests Retrieving the Team Stats
    """
    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: match_up)

    result = download_stats.get_game_info('123456', 2024, 1, '2')
    assert_that(result).is_not_none()
    assert_that(len(result)).is_greater_than(0)


def test_get_game_info_no_response(monkeypatch):
    """
    Tests Retrieving the Stats with no results.
    """

    monkeypatch.setattr(GameService, 'get_game_info', lambda *args: None)

    result = download_stats.get_game_info('123456', 2024, 1, '2')
    assert_that(result).is_none()


def test_main(match_up, monkeypatch, schedule_frame, tmp_path):
    """
    Tests the Main Function
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: match_up)
    schedule_path = os.path.join(tmp_path.as_posix(), 'schedules', 'year=2024', 'type=2',
                                 'week_1.parquet')
    output_path = os.path.join(tmp_path.as_posix(), 'games', 'year=2024', 'type=2',
                               'week-1.parquet')

    download_stats.main(schedule_path, output_path, 'game')

    assert_that(os.listdir(os.path.join(tmp_path.as_posix(), 'games', 'year=2024', 'type=2'))) \
        .contains('week-1.parquet')


def test_main_no_schedule_file(match_up, monkeypatch, tmp_path):
    """
    Tests no schedule file
    """

    output_path = os.path.join(tmp_path.as_posix(), 'games', 'year=2024', 'type=2',
                               'week-1.parquet')
    assert_that(download_stats.main) \
        .raises(SystemExit) \
        .when_called_with('fart', output_path, 'game')


def test_main_no_schedule_frame(monkeypatch, schedule_frame, tmp_path, caplog):
    """
    Tests no schedule frame returned
    """

    schedule_path = os.path.join(tmp_path.as_posix(), 'schedules', 'year=2024', 'type=2',
                                 'week_1.parquet')
    monkeypatch.setattr(download_stats, 'load_schedule_file', lambda *args: None)
    output_path = os.path.join(tmp_path.as_posix(), 'games', 'year=2024', 'type=2',
                               'week-1.parquet')
    assert_that(download_stats.main) \
        .raises(SystemExit) \
        .when_called_with(schedule_path, output_path, 'game')

    assert_that(caplog.text).contains('Schedule file is empty:')
