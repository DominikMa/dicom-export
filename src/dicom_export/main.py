import codecs
from datetime import datetime, timedelta

from dicom_export.get_all_xrays_and_srs import get_all
from dicom_export.utils import get_series, move_series

codecs.register_error('slashescape', codecs.lookup_error('backslashreplace'))

PACS_SETTINGS = {
    'executable': '/usr/bin/findscu',
    'aec': '',
    'aet': '',
    'serverIP': '',
    'serverPort': '11112',
    'then': ''
}

PACS_SETTINGS_MOVE = {
    'executable': '/usr/bin/movescu',
    'aec': '',
    'aet': '',
    'serverIP': '',
    'serverPort': '11112',
    'then': ''
}

# output parameters
OUTPUT_SETTINGS = {
    'printReport': 'json',
    'withFeedBack': False,
}


def get_thorax(limit=10000):
    limit_reached = False
    moved_crs = set()
    search_date = datetime.today()

    while not limit_reached:
        search_date = search_date - timedelta(days=1)
        crs = \
        get_series(modality='CR', study_description='Thorax auf Station', study_date=search_date.strftime('%Y%m%d'))[
            'data']
        for cr in crs:
            move_series(study_instance_uid=cr['StudyInstanceUID']['value'],
                        series_instance_uid=cr['SeriesInstanceUID']['value'])
            moved_crs.add(cr['SeriesInstanceUID']['value'])
            if len(moved_crs) >= limit:
                limit_reached = True
                break


def main():
    # get_thorax(limit=10)
    get_all(end_date=datetime(year=2023, month=6, day=10))
    raise
    # dxs = get_series(modality='DX', study_date='20230612')['data']
    # dx_srs = list(map(lambda e: get_series(modality='SR', study_instance_uid=e['StudyInstanceUID']['value'])['data'], dxs))

    crs = get_series(modality='CR', study_date='20230612')['data']
    cr_srs = list(
        map(lambda e: get_series(modality='SR', study_instance_uid=e['StudyInstanceUID']['value'])['data'], crs))
    for cr, srs in zip(crs, cr_srs):
        move_series(study_instance_uid=cr['StudyInstanceUID']['value'],
                    series_instance_uid=cr['SeriesInstanceUID']['value'])
        for sr in srs:
            move_series(study_instance_uid=sr['StudyInstanceUID']['value'],
                        series_instance_uid=sr['SeriesInstanceUID']['value'])
        print()

    srs = get_sr()

    print()
    # pacs_settings = {
    #     'executable': '/usr/bin/findscu',
    #     'aec': 'TESTEX123',
    #     'aet': 'TESTEX123',
    #     'serverIP': 'www.dicomserver.co.uk',
    #     'serverPort': '11112',
    #     'then': ''
    # }
    #
    # # query parameters
    # query_settings = {
    #     'PatientName': '*',
    #     'SeriesInstanceUID': '',
    #     'QueryRetrieveLevel': 'Study',
    # }
    #
    # # output parameters
    # output_settings = {
    #     'printReport': 'json',
    #     'colorize': 'dark',
    #     'withFeedBack': False,
    # }
    #
    # # python 3.5 ** syntax
    # find = Test({
    #     # find = pypx.Find({
    #     **pacs_settings,
    #     **query_settings,
    #     **output_settings}
    # )
    #
    # output = await find.run({
    #     **pacs_settings,
    #     **query_settings,
    #     **output_settings}
    # )
    #
    # output = await pypx.find({
    #     **pacs_settings,
    #     **query_settings,
    #     **output_settings})

    print()


if __name__ == '__main__':
    main()
