import itertools
import json
from datetime import datetime, timedelta, timezone
from multiprocessing.pool import Pool
from pathlib import Path
from time import sleep
from typing import Dict, Optional

from tqdm import tqdm

from utils import get_series, move_series

DAYS_DONE_FILE_PATH = Path('days_done.json')
FAILED_SERIES_FILE_PATH =  Path('failed_series.json')

START_WORKING_TIME = datetime(year=2000, month=1, day=1, hour=18, minute=0).time()
END_WORKING_TIME = datetime(year=2000, month=1, day=1, hour=6, minute=0).time()


def get_all(
        pacs_settings: Dict,
        pacs_move_settings: Dict,
        output_settings: Dict,
        start_date: datetime,
        end_date: datetime,
):
    """
    Finds and moves all DX, CR and SR files.

    Search moves backwards in time, so `start_date` is after `end_date`.
    Search and move is per day. The search is exclusive `start_date` and inclusive `end_date`.

    :param start_date: 
    :param end_date:
    """

    search_date = start_date
    additional_query_keys={'StudyInstanceUID': '', 'SeriesInstanceUID': ''}

    failed_series = []
    days_done = []
    while search_date > end_date:
        search_date = search_date - timedelta(days=1)
        for modality in ['DX', 'CR']:
            images_per_day = get_series(
                pacs_settings, output_settings,
                modality=modality, study_date=search_date.strftime('%Y%m%d'),
                additional_query_keys=additional_query_keys
            )['data']
            
            study_instance_uid_per_day = set(
                map(lambda e: e['StudyInstanceUID']['value'], images_per_day)
            )
            reports_per_day = list(
                map(lambda study_instance_uid: get_series(
                    pacs_settings, output_settings,
                    modality='SR', study_instance_uid=study_instance_uid,
                    additional_query_keys=additional_query_keys, query_retrieve_level='IMAGE'
                )['data'],
                    study_instance_uid_per_day)
            )
            for image in images_per_day:
                result = move_series(
                    pacs_move_settings, output_settings,
                    study_instance_uid=image['StudyInstanceUID']['value'],
                    series_instance_uid=image['SeriesInstanceUID']['value']
                )
                if result['status'] != 'success' or result['returncode'] != 0:
                    failed_series.append({'SeriesInstanceUID': image['SeriesInstanceUID']['value'], 'StudyInstanceUID': image['StudyInstanceUID']['value']})
                    # raise RuntimeError(f"\nMove error: {result['data']}Got status {result['status']} and returncode {result['returncode']} "
                    #                    f"while moving image StudyInstanceUID {image['StudyInstanceUID']['value']} and "
                    #                    f"SeriesInstanceUID {image['SeriesInstanceUID']['value']}\nSkipping day {search_date}\n\n" 
                    #                    )
            
            for report in itertools.chain.from_iterable(reports_per_day):
                result = move_series(
                    pacs_move_settings, output_settings,
                    study_instance_uid=report['StudyInstanceUID']['value'],
                    series_instance_uid=report['SeriesInstanceUID']['value'],
                    additional_query_keys=additional_query_keys
                )
                if result['status'] != 'success' or result['returncode'] != 0:
                    failed_series.append({'SeriesInstanceUID': report['SeriesInstanceUID']['value'], 'StudyInstanceUID': report['StudyInstanceUID']['value']})
                    # raise RuntimeError(f"\nMove error: {result['data']}Got status {result['status']} and returncode {result['returncode']} "
                    #                    f"while moving report StudyInstanceUID {report['StudyInstanceUID']['value']} and " 
                    #                    f"SeriesInstanceUID {report['SeriesInstanceUID']['value']}\nSkipping day {search_date}\n\n" 
                    #                    )

        days_done.append(search_date.date())
    return (days_done, failed_series,)


def write_done_days(args):
    days_done, failed_series = args
    days_done = list(map(str, days_done))
    days_done_file: list = json.loads(DAYS_DONE_FILE_PATH.read_text())
    days_done_file.extend(days_done)
    DAYS_DONE_FILE_PATH.write_text(json.dumps(days_done_file))
    
    failed_series_file: list = json.loads(FAILED_SERIES_FILE_PATH.read_text())
    failed_series_file.extend(failed_series)
    FAILED_SERIES_FILE_PATH.write_text(json.dumps(failed_series_file))


def error_callback(error):
    print(error)


def populate_pool(
        pool: Pool,
        pacs_settings: Dict,
        pacs_move_settings: Dict,
        output_settings: Dict,
        start_date: datetime,
        end_date: datetime
):
    if DAYS_DONE_FILE_PATH.is_file():
        days_done = json.loads(DAYS_DONE_FILE_PATH.read_text())
    else:
        days_done = []
        DAYS_DONE_FILE_PATH.write_text(json.dumps(days_done))

    if FAILED_SERIES_FILE_PATH.is_file():
        failed_series = json.loads(FAILED_SERIES_FILE_PATH.read_text())
    else:
        failed_series = []
        FAILED_SERIES_FILE_PATH.write_text(json.dumps(failed_series))

    results = []
    part_end_date = end_date
    part_start_date = part_end_date + timedelta(days=1)
    while part_end_date < start_date:
        if str(part_end_date.date()) not in days_done:  
            result = pool.apply_async(
                get_all,
                kwds={
                    'pacs_settings': pacs_settings,
                    'pacs_move_settings': pacs_move_settings,
                    'output_settings': output_settings,
                    'start_date': part_start_date,
                    'end_date': part_end_date,
                },
                callback=write_done_days,
                error_callback=error_callback
            )
            results.append(result)

        part_end_date = part_start_date
        part_start_date = part_end_date + timedelta(days=1)


def run_get_all(
        pacs_settings: Dict,
        pacs_move_settings: Dict,
        output_settings: Dict,
        start_date: datetime,
        end_date: datetime,
):
    t = tqdm(total=(start_date - end_date).days)
    pool = None

    if DAYS_DONE_FILE_PATH.is_file():
        days_done = json.loads(DAYS_DONE_FILE_PATH.read_text())
    else:
        days_done = []
        DAYS_DONE_FILE_PATH.write_text(json.dumps(days_done))

    if FAILED_SERIES_FILE_PATH.is_file():
        failed_series = json.loads(FAILED_SERIES_FILE_PATH.read_text())
    else:
        failed_series = []
        FAILED_SERIES_FILE_PATH.write_text(json.dumps(failed_series))
    
    while True:
        now = datetime.now(timezone(timedelta(hours=2))).time()
        if now > END_WORKING_TIME and now < START_WORKING_TIME and pool is not None:
            print('Stopped pool')
            pool.terminate()
            pool = None
        if (now < END_WORKING_TIME or now > START_WORKING_TIME) and pool is None:
            print('Started pool')
            pool = Pool(2)
            populate_pool(
                pool,
                pacs_settings,
                pacs_move_settings,
                output_settings,
                start_date,
                end_date
            )
        sleep(10)
        days_done = json.loads(DAYS_DONE_FILE_PATH.read_text())
        failed_series = json.loads(FAILED_SERIES_FILE_PATH.read_text())
        t.update(len(days_done) - t.n)
        t.set_postfix({'failed series': len(failed_series)})


if __name__ == '__main__':

    pacs_settings = {
        'findscu': 'findscu',
        'movescu': 'movescu',
        'aec': 'IMPAX9346UHL',
        'aet': 'KPBERKEL',
        'serverIP': '10.140.221.63',
        'serverPort': '11112',
        'then': ''
    }

    pacs_move_settings = {
        'findscu': 'findscu',
        'movescu': 'movescu',
        'aec': 'IMPAX9346UHL',
        'aet': 'KPBERKEL',
        'serverIP': '10.140.221.63',
        'serverPort': '11112',
        'then': ''
    }

    # output parameters
    output_settings = {
        'printReport': 'json',
        'colorize': 'dark',
        'withFeedBack': False,
    }

    # run_get_all(pacs_settings, pacs_move_settings, output_settings,
    #             start_date=datetime(day=1, month=2, year=2021),
    #             end_date=datetime(day=1, month=1, year=2020),
    #             )
    
    run_get_all(pacs_settings, pacs_move_settings, output_settings,
                start_date=datetime(day=1, month=1, year=2023),
                end_date=datetime(day=1, month=1, year=2022),
                )

    print()
