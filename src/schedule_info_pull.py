"""
Script to pull Game information and combine in a single parquet file.
"""
import argparse
import logging
import os.path
import sys
import polars
from io import BytesIO
from boto3 import Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError
import os
from typing import NamedTuple

from services.stats import ScheduleService

class GameType(NamedTuple):
    """
    Game Type Tuple Object
    """
    type_id: int
    game_type: str


def create_client(session:Session) -> BaseClient:
    """
    Creates an S3 Client
    :param session: Boto3 Session
    :return: S3 Client
    """

    if os.getenv('S3_ENDPOINT'):
        return session.client('s3', endpoint_url=os.getenv('S3_ENDPOINT'))
    return session.client('s3')


def get_game_types() -> list[GameType]:
    """
    Populates a List of Game Types
    :return: List of Game Types
    """

    return [
        GameType(1,'preseason'),
        GameType(2,'regular'),
        GameType(3,'postseason')
    ]


def get_schedule(year: int, week: int, game_type: int) -> list[dict]:
    """
    Retrieves the Schedule for a given Season Week.
    :param year: Year Value
    :param week: Week Value
    :param game_type: Game Type
    :return: List of Game Stats
    """
    service = ScheduleService()
    return service.get_schedule(week, year, game_type)


def get_weeks(year: int, game_type: int) -> list[int]:
    """
    Returns a list of Weeks
    :param year: Year Value
    :param game_type: Type of Season (Pre, Reg, Post)
    :return: List of Int
    """

    if game_type == 1:
        return list(range(1, 5))

    if game_type == 3:
        if year <= 2009:
            return list(range(1, 5))
        return [1, 2, 3, 5]

    if year <= 2020:
        return list(range(1, 18))

    return list(range(1, 19))


def write_output(bucket: str, key: str, records: list[dict], session: Session) -> None:
    """
    Writes the Output Parquet File to S3 Storage
    :param bucket: S3 Bucket
    :param key: S3 Key
    :param records: Records
    :param session: Boto3 Session
    :return: None
    """

    client = create_client(session)
    stream = BytesIO()
    frame = polars.DataFrame(records)

    try:
        frame.write_parquet(stream, compression='snappy')
        client.put_object(Bucket=bucket, Key=key, Body=stream.getvalue())
    except ClientError as ex:
        logging.error('Failed to write schedule parquet: %s : %s', key, ex.args)
        raise ex


def main(bucket:str, year: int, **kwargs) -> None:
    """
    Main Function to download Game Stats and output to a parquet file
    :param bucket: S3 Bucket
    :param year: Year Value
    :keyword week: Optional Week Value
    :keyword type: Optional Game Type
    :return: None
    """

    logger = logging.getLogger(__name__)

    week_number = int(kwargs.get('week', 0))
    game_type = int(kwargs.get('type', 0))
    session = Session()

    if not year:
        logger.error('Year Value is Missing')
        sys.exit()

    if not bucket:
        logger.error('Output Bucket is Missing')
        sys.exit()

    game_types = get_game_types()
    if game_type and game_type != 0:
        game_types = [x for x in game_types if x.type_id == game_type]

    logger.info('Retrieving Schedule for %s', year)
    for gt in game_types:
        weeks = get_weeks(year, gt.type_id)
        if week_number and week_number != 0:
            weeks = [int(week_number)]

        for wk in weeks:
            output_key = f"schedules/{year}/{gt.game_type}/week_{wk}.parquet"
            records = get_schedule(year, wk, gt.type_id)
            if not records:
                logger.error('Failed to retrieve Schedule for Type %s : Week %s', gt.game_type, wk)
                continue

            logger.info('Writing Output %s', output_key)
            write_output(bucket, output_key, records, session)
    logger.info('Done')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", type=int, help="Year Value", required=True)
    parser.add_argument("-b", "--bucket", type=str, help="Output Bucket", required=True)
    parser.add_argument('-t', '--type', type=int, help='Game Type', required=False)
    parser.add_argument('-w', '--week', type=str, help='Week Value', required=False)

    args = parser.parse_args()
    cli_args = vars(args)
    main(**cli_args)
