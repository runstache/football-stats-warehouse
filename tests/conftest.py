"""
Pytest Fixtures.
"""

import json
import os

import pandas
import pytest

MATCH_UP_FILE = './tests/test_files/team-output.json'
BOX_SCORE_FILE = './tests/test_files/box-output.json'
SCHEDULE_FILE = './tests/test_files/schedule-output.json'


def load_json_file(path: str) -> dict:
    """
    Loads a JSON File
    :param path: Path to File
    :return: Dictionary
    """

    with open(path) as file:
        return json.load(file)


@pytest.fixture(scope='function')
def match_up() -> dict:
    """
    Registers the MatchUp
    """

    return load_json_file(MATCH_UP_FILE)


@pytest.fixture(scope='function')
def box_score() -> dict:
    """
    Registers the Box Score.
    """

    return load_json_file(BOX_SCORE_FILE)


@pytest.fixture(scope='function')
def schedule() -> dict:
    """
    Registers the Schedule file.
    """
    return load_json_file(SCHEDULE_FILE)


@pytest.fixture
def schedule_frame(tmp_path) -> pandas.DataFrame:
    """
    Registers a Schedule Data Frame
    """

    record = {
        'game_id': ['123445'],
        'home_team_code': ['BUF'],
        'home_team': ['Buffalo Bills'],
        'away_team_code': ['PIT'],
        'away_team': ['Pittsburgh Steelers'],
        'year': ['2024'],
        'week': ['1'],
        'game_type': ['2'],
        'game_date': ['20240101']
    }
    df = pandas.DataFrame.from_dict(record)
    output_path = os.path.join(tmp_path.as_posix(), 'schedules', 'year=2024', 'type=2')
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    df.to_parquet(
        os.path.join(output_path, 'week_1.parquet'),
        engine='pyarrow')
    return df
