import itertools
import json
from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Dict, Optional

from dicom_export.utils import get_series, move_series

DAYS_DONE_FILE_PATH = Path('days_done.json')


def get_all(
        pacs_settings: Dict,
        pacs_move_settings: Dict,
        output_settings: Dict,
        start_date: datetime,
        end_date: datetime,
        sop_class_uid_postfix: Optional[str] = None
):
    """
    Finds and moves all DX, CR and SR files.

    Search moves backwards in time, so `start_date` is after `end_date`.
    Search and move is per day. The search is exclusive `start_date` and inclusive `end_date`.

    :param start_date: 
    :param end_date:
    """

    search_date = start_date

    days_done = []
    while search_date > end_date:
        search_date = search_date - timedelta(days=1)
        for modality in ['DX', 'CR']:
            pass
            # images_per_day = get_series(
            #     pacs_settings, output_settings,
            #     modality=modality, study_date=search_date.strftime('%Y%m%d')
            # )['data']

            # if sop_class_uid_postfix is not None:
            #     additional_query_keys = {'SOPClassUID': f'*{sop_class_uid_postfix}'}
            # else:
            #     additional_query_keys = None
            #
            # reports_per_day = list(
            #     map(lambda e: get_series(
            #         pacs_settings, output_settings,
            #         modality='SR', study_instance_uid=e['StudyInstanceUID']['value'],
            #         additional_query_keys=additional_query_keys
            #     )['data'],
            #         images_per_day)
            # )

            # for image in images_per_day:
            #     move_series(
            #         pacs_move_settings, output_settings,
            #         study_instance_uid=image['StudyInstanceUID']['value'],
            #         series_instance_uid=image['SeriesInstanceUID']['value']
            #     )
            #
            # for report in itertools.chain.from_iterable(reports_per_day):
            #     move_series(
            #         pacs_move_settings, output_settings,
            #         study_instance_uid=report['StudyInstanceUID']['value'],
            #         series_instance_uid=report['SeriesInstanceUID']['value'],
            #         additional_query_keys=additional_query_keys
            #     )

        days_done.append(search_date.date())
    return days_done


def write_done_days(days_done):
    days_done = list(map(str, days_done))
    print(days_done)


def error_callback(days_done):
    days_done = list(map(str, days_done))
    print(days_done)


def run_get_all(
        pacs_settings: Dict,
        pacs_move_settings: Dict,
        output_settings: Dict,
        start_date: datetime,
        end_date: datetime,
        sop_class_uid_postfix: Optional[str] = None
):
    if DAYS_DONE_FILE_PATH.is_file():
        days_done = set(json.loads(DAYS_DONE_FILE_PATH.read_text()))
    else:
        days_done = set()
        DAYS_DONE_FILE_PATH.write_text(json.dumps(list(days_done)))

    pool = Pool(1)
    results = []
    part_end_date = end_date
    part_start_date = part_end_date + timedelta(days=1)
    while part_end_date < start_date:
        # get_all(*(pacs_settings,
        #      pacs_move_settings,
        #      output_settings,
        #      part_start_date,
        #      part_end_date,
        #      sop_class_uid_postfix))
        #
        # print()
        result = pool.apply_async(
            get_all
            (pacs_settings,
             pacs_move_settings,
             output_settings,
             part_start_date,
             part_end_date,
             sop_class_uid_postfix),
            callback=write_done_days,
            error_callback=error_callback
        )
        results.append(result)

        part_end_date = part_start_date
        part_start_date = part_end_date + timedelta(days=1)

    pool.close()
    pool.join()


if __name__ == '__main__':
    pacs_settings = {
        'findscu': 'findscu',
        'movescu': 'movescu',
        'aec': 'TESTEX123',
        'aet': 'TESTEX123',
        'serverIP': 'www.dicomserver.co.uk',
        'serverPort': '11112',
        'then': ''
    }

    # output parameters
    output_settings = {
        'printReport': 'json',
        'colorize': 'dark',
        'withFeedBack': False,
    }

    run_get_all(pacs_settings, pacs_settings, output_settings,
                start_date=datetime(day=1, month=1, year=2001),
                end_date=datetime(day=1, month=1, year=2000),
                sop_class_uid_postfix='.11')

    print()
