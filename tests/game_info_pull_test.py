"""
Tests for the Teams Status Data Pull
"""

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


def test_main(match_up, monkeypatch, s3, session):
    """
    Tests the Main Function
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: match_up)

    schedule_key = 'schedules/2020/1/week_1.parquet'
    bucket = 'warehouse-bucket'

    download_stats.main(bucket, schedule_key, 'games')

    client = session.client('s3')
    response = client.list_objects_v2(Bucket=bucket, Prefix='games/2020/1/')
    assert_that(response.get('Contents', [])).is_not_empty()


def test_main_no_schedule_file(match_up, monkeypatch, s3, session):
    """
    Tests no schedule file
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: match_up)

    schedule_key = 'schedules/2020/1/week_2.parquet'
    bucket = 'warehouse-bucket'

    assert_that(download_stats.main) \
        .raises(SystemExit) \
        .when_called_with(bucket, schedule_key, 'games')
