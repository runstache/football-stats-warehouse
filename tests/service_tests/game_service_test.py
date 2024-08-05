"""
Tests for the Game Service.
"""

from assertpy import assert_that

from services.stats import GameService, BaseService


def test_get_game_info(monkeypatch, match_up):
    """
    Tests retrieving the Game Info
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: match_up)

    service = GameService()
    result = service.get_game_info('12345', 1, 2023, 'postseason')

    assert_that(result).is_not_none()

    assert_that(result).contains_entry({'home_team': 'Baltimore Ravens'}) \
        .contains_entry({'game_id': '12345'}) \
        .contains_entry({'away_team': 'Kansas City Chiefs'}) \
        .contains_entry({'location': 'M&T Bank Stadium'}) \
        .contains_entry({'city': 'Baltimore'}) \
        .contains_entry({'state': 'MD'}) \
        .contains_entry({'game_date': '2024-01-28T20:00Z'}) \
        .contains_entry({'week': 1}) \
        .contains_entry({'year': 2023}) \
        .contains_entry({'game_type': 'postseason'}) \
        .contains_entry({'is_conference': False}) \
        .contains_entry({'note': 'AFC Championship'}) \
        .contains_entry({'home_score': 10}) \
        .contains_entry({'away_score': 17}) \
        .contains_entry({'line': '-4.5'}) \
        .contains_entry({'over_under': 43.5})


def test_get_game_info_no_payload(monkeypatch):
    """
    Tests when no payload is received from the Service.,
    """

    monkeypatch.setattr(BaseService, 'get_stats_payload', lambda a, b: {})

    service = GameService()
    result = service.get_game_info('12345', 1, 2023, 'postseason')

    assert_that(result).is_none()
