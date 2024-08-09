"""
Pytest Fixtures.
"""

import pytest
import json

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
