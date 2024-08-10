"""
Script to pull Game information and combine in a single parquet file.
"""
import argparse
import logging
import os.path
import sys

import pandas

from services.stats import ScheduleService


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


def write_output(path: str, records: list[dict]) -> None:
    """
    Writes the Output Parquet File
    :param path: Output Path
    :param records: Records to output
    :return: None
    """

    output_file_name = os.path.basename(path)
    output_directory = path.replace(output_file_name, '')

    df = pandas.DataFrame.from_records(records)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    df.to_parquet(path=path, engine='pyarrow', index=False)


def main(year: int, output: str, **kwargs) -> None:
    """
    Main Function to download Game Stats and output to a parquet file
    :param year: Year Value
    :param output: Output Path
    :keyword week: Optional Week Value
    :keyword type: Optional Game Type
    :return: None
    """

    logger = logging.getLogger(__name__)

    week_number = kwargs.get('week', 0)
    game_type = kwargs.get('type', 0)

    if not year or year == 0:
        logger.error('Year Value is Missing')
        sys.exit()

    if not output:
        logger.error('Output Path is Missing')
        sys.exit()

    if game_type != 0:
        game_types = [int(game_type)]
    else:
        game_types = list(range(1, 4))

    logger.info('Retrieving Schedule for %s', year)
    for gt in game_types:
        if week_number != 0:
            weeks = [int(week_number)]
        else:
            weeks = get_weeks(year, gt)
        for wk in weeks:
            output_path = os.path.join(output, 'schedules', f"year={year}", f"type={gt}",
                                       f"week_{wk}.parquet")
            records = get_schedule(year, wk, gt)
            if not records:
                logger.error('Failed to retrieve Schedule for Type %s : Week %s', gt, wk)
                continue

            logger.info('Writing Output %s', output_path)
            write_output(output_path, records)
    logger.info('Done')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", type=int, help="Year Value", required=True)
    parser.add_argument("-o", "--output", type=str, help="Output Path", required=True)
    parser.add_argument('-t', '--type', type=str, help='Game Type', required=False)
    parser.add_argument('-w', '--week', type=str, help='Week Value', required=False)

    args = parser.parse_args()
    cli_args = vars(args)
    main(**cli_args)
