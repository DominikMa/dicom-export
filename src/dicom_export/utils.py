import collections
from typing import Optional, Dict

import pypx


class Find(pypx.Find):

    def query(self, opt=None):
        if opt is None:
            return ''

        query = ''

        for key, value in sorted(opt.items()):
            if value != '':
                query += f' -k "{key}={value}"'
            else:
                query += f' -k {key}'

        return query


def get_series(
        pacs_settings: Dict,
        output_settings: Dict,
        modality: Optional[str] = None,
        study_instance_uid: Optional[str] = None,
        study_date: Optional[str] = None,
        series_description: Optional[str] = None,
        study_description: Optional[str] = None,
        additional_query_keys: Optional[Dict[str, str]] = None
):
    # query parameters
    query_settings = {}
    if additional_query_keys is not None:
        query_settings.update(additional_query_keys)
    if modality is not None:
        query_settings['Modality'] = modality
    if study_instance_uid is not None:
        query_settings['StudyInstanceUID'] = study_instance_uid
    if study_date is not None:
        query_settings['StudyDate'] = study_date
    if series_description is not None:
        query_settings['SeriesDescription'] = series_description
    if study_description is not None:
        query_settings['StudyDescription'] = study_description

    settings = {
        **pacs_settings,
        **output_settings,
        **query_settings,
    }

    find = Find(settings)

    formattedStudiesResponse = find.systemlevel_run(
        query_settings,
        {
            'f_commandGen': find.findscu_command,
            'QueryRetrieveLevel': 'SERIES'
        }
    )
    return formattedStudiesResponse


def move_series(
        pacs_settings: Dict,
        output_settings: Dict,
        study_instance_uid: str,
        series_instance_uid: str,
        additional_query_keys: Optional[Dict[str, str]] = None
):
    # query parameters
    query_settings = {}
    if additional_query_keys is not None:
        query_settings.update(additional_query_keys)
    query_settings['StudyInstanceUID'] = study_instance_uid
    query_settings['SeriesInstanceUID'] = series_instance_uid

    settings = {
        **pacs_settings,
        **output_settings,
        **query_settings,
    }

    move = pypx.Move(settings)
    move.run(query_settings)
