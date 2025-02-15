"""
Pytest Fixtures.
"""

import json
import os

import boto3
import polars
import pytest
from moto import mock_aws
from io import BytesIO


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
def schedule_frame(tmp_path) -> polars.DataFrame:
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
    df = polars.DataFrame(record)
    return df


@pytest.fixture
def aws_credentials():
    """
    Sets up fake credentials
    """

    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture
def session(aws_credentials):
    """
    Sets up a mock session
    """
    with mock_aws():
        yield boto3.Session(region_name='us-east-1')


@pytest.fixture
def s3(session, schedule_frame):
    """
    Creates Mock S3 Buckets
    """

    client = session.client('s3')
    client.create_bucket(Bucket='warehouse-bucket')

    stream = BytesIO()
    schedule_frame.write_parquet(stream)
    client.put_object(Bucket='warehouse-bucket', Key='schedules/2020/1/week_1.parquet', Body=stream.getvalue())




