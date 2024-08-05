"""
Tests for the ScheduleService class.
"""
import json

import pytest
from assertpy import assert_that

from services.stats import ScheduleService


def test_get_schedule(schedule, monkeypatch):
    """
    Tests retrieving the schedule entries.
    """

    service = ScheduleService()
    monkeypatch.setattr(service, 'get_stats_payload', lambda a: schedule)

    result = service.get_schedule(1, 2023, 1)

    assert_that(result).is_not_empty()

    assert_that(result[0]).contains_entry({'game_id': '401671807'}) \
        .contains_entry({'year': 2023}) \
        .contains_entry({'week': 1}) \
        .contains_entry({'game_type': 1}) \
        .contains_entry({'home_team_code': 'MIA'}) \
        .contains_entry({'home_team': 'Miami Dolphins'}) \
        .contains_entry({'away_team_code': 'BUF'}) \
        .contains_entry({'away_team': 'Buffalo Bills'}) \
        .contains_entry({'game_date': '20240912'})


def test_get_schedules_no_response(monkeypatch):
    """
    Tests No Response from the service
    """

    service = ScheduleService()
    monkeypatch.setattr(service, 'get_stats_payload', lambda a: None)

    result = service.get_schedule(1, 2024, 1)
    assert_that(result).is_empty()
