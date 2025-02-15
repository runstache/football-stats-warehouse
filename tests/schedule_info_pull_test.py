"""
Tests for the Game Info retrieval script
"""

from assertpy import assert_that

import schedule_info_pull
from services.stats import BaseService


def test_get_schedule(monkeypatch, schedule):
    """
    Tests Retrieving the Game Stats from the Web
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: schedule)

    result = schedule_info_pull.get_schedule(2023, 1, 2)
    assert_that(result).is_not_empty()
    assert_that(result).is_length(1)


def test_main(monkeypatch, tmp_path, schedule, s3, session):
    """
    Tests the Main Function
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: schedule)
    schedule_info_pull.main('warehouse-bucket', 2023)

    client = session.client('s3')
    response = client.list_objects_v2(Bucket='warehouse-bucket', Prefix='schedules/2023/preseason')
    assert_that(response.get('Contents', [])).is_not_empty()

    response = client.list_objects_v2(Bucket='warehouse-bucket', Prefix='schedules/2023/regular')
    assert_that(response.get('Contents', [])).is_not_empty()

    response = client.list_objects_v2(Bucket='warehouse-bucket', Prefix='schedules/2023/postseason')
    assert_that(response.get('Contents', [])).is_not_empty()


def test_main_no_year(tmp_path, caplog, s3, session):
    """
    Tests the main function with an invalid year
    """
    assert_that(schedule_info_pull.main) \
        .raises(SystemExit) \
        .when_called_with('warehouse-bucket', 0)

    assert_that(caplog.text).contains('Year Value is Missing')


def test_main_no_output(caplog, s3, session):
    """
    Tests the main function with no output parameter
    """
    assert_that(schedule_info_pull.main) \
        .raises(SystemExit) \
        .when_called_with('', 2023)

    assert_that(caplog.text).contains('Output Bucket is Missing')


def test_main_week_type(monkeypatch, schedule, s3, session):
    """
    Tests the main function with the Week and Type Keyword Arguments
    """
    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda *args: schedule)

    args = {
        'bucket': 'warehouse-bucket',
        'year': 2023,
        'week': 1,
        'type': 1
    }

    schedule_info_pull.main(**args)

    client = session.client('s3')
    response = client.list_objects_v2(Bucket='warehouse-bucket',
                                      Prefix='schedules/2023/preseason/week_1.parquet')
    assert_that(response.get('Contents', [])).is_not_empty()


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


def test_main_week_failure(monkeypatch, s3, session, caplog):
    """
    Tests the main function with a failed week
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: None)
    args = {
        'bucket': 'warehouse-bucket',
        'year': 2023
    }
    schedule_info_pull.main(**args)

    client = session.client('s3')
    response = client.list_objects_v2(Bucket='warehouse-bucket', Prefix='schedules/2023/')
    assert_that(response.get('Contents', [])).is_empty()

    assert_that(caplog.text).contains('Failed to retrieve Schedule')
