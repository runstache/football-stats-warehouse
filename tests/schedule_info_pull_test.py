"""
Tests for the Game Info retrieval script
"""
import os.path

from assertpy import assert_that

import schedule_info_pull
from services.stats import BaseService, ScheduleService


def test_get_schedule(monkeypatch, schedule):
    """
    Tests Retrieving the Game Stats from the Web
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: schedule)

    result = schedule_info_pull.get_schedule(2023, 1, 2)
    assert_that(result).is_not_empty()
    assert_that(result).is_length(1)


def test_main(monkeypatch, tmp_path, schedule):
    """
    Tests the Main Function
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: schedule)
    schedule_info_pull.main(2023, tmp_path.as_posix())

    pre_season = [f"week_{x}.parquet" for x in range(1, 5)]
    reg_season = [f"week_{x}.parquet" for x in range(1, 19)]
    post_season = [f"week_{x}.parquet" for x in [1, 2, 3, 5]]

    assert_that(os.listdir(os.path.join(tmp_path.as_posix(), 'year=2023'))) \
        .contains_only('type=1', 'type=2', 'type=3')
    assert_that(os.listdir(os.path.join(tmp_path.as_posix(), 'year=2023', 'type=1'))) \
        .contains_only(*pre_season)
    assert_that(os.listdir(os.path.join(tmp_path.as_posix(), 'year=2023', 'type=2'))) \
        .contains_only(*reg_season)
    assert_that(os.listdir(os.path.join(tmp_path.as_posix(), 'year=2023', 'type=3'))) \
        .contains_only(*post_season)


def test_main_no_year(tmp_path, caplog):
    """
    Tests the main function with an invalid year
    """
    assert_that(schedule_info_pull.main) \
        .raises(SystemExit) \
        .when_called_with(0, tmp_path.as_posix())

    assert_that(caplog.text).contains('Year Value is Missing')


def test_main_no_output(caplog):
    """
    Tests the main function with no output parameter
    """
    assert_that(schedule_info_pull.main) \
        .raises(SystemExit) \
        .when_called_with(2023, '')

    assert_that(caplog.text).contains('Output Path is Missing')


def test_main_week_type(monkeypatch, tmp_path, schedule):
    """
    Tests the main function with the Week and Type Keyword Arguments
    """
    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: schedule)
    schedule_info_pull.main(2023, tmp_path.as_posix(), week='1', type='1')

    assert_that(os.path.exists(
        os.path.join(tmp_path.as_posix(), 'year=2023', 'type=1', 'week_1.parquet'))).is_true()

    assert_that(os.path.exists(
        os.path.join(tmp_path.as_posix(), 'year=2023', 'type=1', 'week_2.parquet'))).is_false()


def test_get_weeks_17():
    """
    Tests the correct number of weeks is returned for 17 week seasons
    """
    result = schedule_info_pull.get_weeks(2015, 2)
    assert_that(result).does_not_contain(18)


def test_get_weeks_18():
    """
    Tests the correct number of weeks is returns for 18 week seasons
    """
    result = schedule_info_pull.get_weeks(2022, 2)
    assert_that(result).contains(18)


def test_get_weeks_preseason():
    """
    Tests retrieving the Preseason Weeks
    """
    result = schedule_info_pull.get_weeks(2022, 1)
    assert_that(result).contains_only(1, 2, 3, 4)


def test_get_weeks_post_season():
    """
    Tests Retrieving the Post Season Week after 2009
    :return:
    """
    result = schedule_info_pull.get_weeks(2022, 3)
    assert_that(result).contains_only(1, 2, 3, 5)


def test_get_weeks_post_season_2009():
    """
    Tests retrieving the Post Season Weeks Pre-2009
    """
    result = schedule_info_pull.get_weeks(2008, 3)
    assert_that(result).contains_only(1, 2, 3, 4)


def test_main_week_failure(monkeypatch, caplog, tmp_path, schedule):
    """
    Tests the main function with a failed week
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: None)

    schedule_info_pull.main(2023, tmp_path.as_posix())

    assert_that(os.path.exists(os.path.join(tmp_path.as_posix(), 'year=2023', 'type=1'))).is_false()
    assert_that(os.path.exists(os.path.join(tmp_path.as_posix(), 'year=2023', 'type=2'))).is_false()
    assert_that(os.path.exists(os.path.join(tmp_path.as_posix(), 'year=2023', 'type=3'))).is_false()

    assert_that(caplog.text).contains('Failed to retrieve Schedule')