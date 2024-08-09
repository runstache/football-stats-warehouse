"""
Team Statistics Scraping Script
"""
import sys

from services.stats import TeamService
import pandas
import os
import logging
import argparse

OUTPUT_FOLDER = 'teams'


def load_schedule_file(path: str) -> pandas.DataFrame:
    """
    Loads the Schedule File to a Data Frame
    :param path: Path to Frame
    :return: Data Frame
    """
    return pandas.read_parquet(path, engine='pyarrow')


def get_team_stats(game_id: str, year: int, week: int, game_type: str, ) -> pandas.DataFrame | None:
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
        return pandas.DataFrame.from_records(result)
    return None


def write_output(frame: pandas.DataFrame, path: str) -> None:
    """
    Writes the DataFrame output to Parquet
    :param frame: DataFrame
    :param path: Output Path
    :return: None
    """
    output_file_name = os.path.basename(path)
    output_directory = path.replace(output_file_name, '')

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    frame.to_parquet(path, engine='pyarrow')


def main(source_file: str, output_directory: str) -> None:
    """
    Main Function to pull Team Level Stats
    :param source_file: Schedule Source File Parquet File
    :param output_directory: Output Directory.
    :return: None
    """

    def compile_frames(row, collection: list[pandas.DataFrame]):
        """
        Processes Each Schedule Row and adds the Resulting frame to the collection.
        :param row: Frame Row
        :param collection: Data Frame Collection
        :return: None
        """

        result = get_team_stats(str(row['game_id']), int(row['year']), int(row['week']),
                                str(row['game_type']))
        if result is not None:
            collection.append(result)

    logger = logging.getLogger(__name__)
    logger.info('Processing Schedule File: %s', source_file)

    if not os.path.exists(source_file):
        sys.exit('Schedule File Not Found')

    schedule_frame = load_schedule_file(source_file)

    if schedule_frame is None or len(schedule_frame) == 0:
        logger.warning('Schedule file is empty: %s', source_file)
        sys.exit('No Schedule File Records')

    frames = []

    schedule_frame.apply(lambda row: compile_frames(row, frames), axis=1)

    if not frames:
        logger.warning('No Team Stats Loaded from Schedule File')
        sys.exit(0)

    stats = pandas.concat(frames, ignore_index=True)

    logger.info('Writing Output to %s', output_directory)
    write_output(stats, output_directory)
    logger.info('Done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--schedule", type=str,
                        help='Input Schedule Source File', required=True)
    parser.add_argument('-o', '--output', type=str,
                        help='Output File Path including file name', required=True)

    args = parser.parse_args()
    main(args.schedule, args.output)
