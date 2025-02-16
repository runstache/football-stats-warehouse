"""
Retrieves the Stats for a given Schedule File
"""

import argparse
import logging
import os
import sys
from io import BytesIO

import polars
from boto3 import Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from services.stats import TeamService, PlayerService, GameService


def create_client(session: Session) -> BaseClient:
    """
    Creates a Base S3 Client
    :param session: Boto Session
    :return: S3 Client
    """

    if os.getenv('S3_ENDPOINT'):
        return session.client('s3', endpoint_url=os.getenv('S3_ENDPOINT'))
    return session.client('s3')


def load_schedule_file(bucket: str, key: str, session: Session) -> polars.DataFrame | None:
    """
    Loads the Schedule File from S3 Bucket
    :param bucket: S3 Bucket
    :param key: S3 Key
    :param session: Boto Session
    :return: Optional Data Frame
    """

    try:
        client = create_client(session)
        response = client.get_object(Bucket=bucket, Key=key)

        content = response['Body'].read()
        return polars.read_parquet(content)

    except ClientError as ex:
        logging.error('Failed to load Schedule File: %s : %s', key, ex.args)

    return None


def get_team_stats(game_id: str, year: int, week: int, game_type: str) -> polars.DataFrame | None:
    """
    Returns the Team based Stats from the provided Game ID.
    :param game_id: Game ID
    :param year: Year Value
    :param week: Week Number
    :param game_type: Game Type ID
    :return: Data Frame
    """
    service = TeamService()
    result = service.get_team_stats(game_id, week, year, game_type)
    if result:
        return polars.DataFrame(result)
    return None


def get_player_stats(game_id: str, year: int, week: int, game_type: str) -> polars.DataFrame | None:
    """
    Returns the Player based Stats from the provided Game ID.
    :param game_id: Game ID
    :param year: Year Value
    :param week: Week Number
    :param game_type: Game Type ID
    :return: Data Frame
    """
    service = PlayerService()
    result = service.get_player_stats(game_id, week, year, game_type)
    if result:
        return polars.DataFrame(result)
    return None


def get_game_info(game_id: str, year: int, week: int, game_type: str) -> polars.DataFrame | None:
    """
    Returns the game Info as a Data Frame
    :param game_id: Game ID
    :param year: Year Value
    :param week: Week Number
    :param game_type: Game Type ID
    :return: Data Frame
    """
    service = GameService()
    result = service.get_game_info(game_id, week, year, game_type)

    if not result:
        return None

    return polars.DataFrame([result])


def write_output(frame: polars.DataFrame, bucket: str, key: str, session: Session) -> None:
    """
    Writes the DataFrame output to Parquet in S3 bucket
    :param frame: DataFrame
    :param bucket: S3 Bucket
    :param key: S3 Key
    :param session: Boto Session
    :return: None
    """

    stream = BytesIO()
    frame.write_parquet(stream)

    try:
        client = create_client(session)
        client.put_object(Bucket=bucket, Key=key, Body=stream.getvalue())
    except ClientError as ex:
        logging.error('Failed to write output to S3 bucket: %s : %s', key, ex.args)
        raise ex


def main(bucket: str, schedule_key: str, stat_type: str) -> None:
    """
    Main Function to pull Team Level Stats
    :param bucket: S3 Bucket
    :param schedule_key: S3 Schedule File Key
    :param stat_type: Stats Type, Player or Team
    :return: None
    """

    def compile_frames(row: dict, stat: str):
        """
        Processes Each Schedule Row and adds the Resulting frame to the collection.
        :param row: Dictionary of the Row
        :param stat: Stats Type
        :return: None
        """
        result = None
        if stat == 'teams':
            result = get_team_stats(str(row['game_id']), int(row['year']), int(row['week']),
                                    str(row['game_type']))

        if stat == 'players':
            result = get_player_stats(str(row['game_id']), int(row['year']), int(row['week']),
                                      str(row['game_type']))
        if stat == 'games':
            result = get_game_info(str(row['game_id']), int(row['year']), int(row['week']),
                                   str(row['game_type']))

        return result

    logger = logging.getLogger(__name__)
    logger.info('Processing Schedule File for %s Stats: %s', stat_type, schedule_key)

    if stat_type not in ('players', 'games', 'teams'):
        logging.error('Invalid Stats Type: %s', stat_type)
        sys.exit(0)

    session = Session()
    schedule_frame = load_schedule_file(bucket, schedule_key, session)

    if schedule_frame is None or len(schedule_frame) == 0:
        logger.warning('Schedule file is empty: %s', schedule_key)
        sys.exit('No Schedule File Records')

    frames: list[polars.DataFrame] = []
    rows = schedule_frame.to_dicts()
    frames.extend([compile_frames(x, stat_type) for x in rows])
    frames = [x for x in frames if x is not None]
    if not frames:
        logger.warning('No %s Stats Loaded from Schedule File', stat_type)
        sys.exit(0)
    stats = polars.concat(frames, how='diagonal')

    output_key = schedule_key.replace('schedules', stat_type)

    logger.info('Writing Output to %s', output_key)
    write_output(stats, bucket, output_key, session)
    logger.info('Done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--schedule", type=str,
                        help='Schedule File S3 Key', required=True)
    parser.add_argument('-b', '--bucket', type=str,
                        help='Warehouse S3 Bucket', required=True)
    parser.add_argument('-t', '--stat', type=str, help='Type of Stats to retrieve', required=True)

    args = parser.parse_args()
    main(args.bucket, args.schedule, args.stat)
