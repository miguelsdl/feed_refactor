import datetime
import logging
from sqlalchemy import text
import connections
import xml_mapper
import re

# consulta sql para agrega una constrain para que funcione ON DUPLICATE
# ALTER TABLE feed.tracks ADD CONSTRAINT constr_isrc_track  UNIQUE (isrc_track);

def get_track_list(release_list):
    #  TODO - ver si se puede optimizar este método
    track_list_by_ref = dict()
    trak_release = release_list['TrackRelease']

    # si lo que me viene no es una lista de objetos (viene solo uno) lo convierto a una lista de largo 1
    if not isinstance(trak_release, list):
        trak_release = [trak_release, ]

    # Itero y me acomodo los datos en un dict pot ReleaseResourceReference
    for track_data in trak_release:
        track_list_by_ref[track_data['ReleaseResourceReference']] = track_data

    return track_list_by_ref


def get_recording_list(recording_list):
    #  TODO - ver si se puede optimizar este método
    resource_list_by_ref = dict()
    recording_list = recording_list['SoundRecording']

    # si lo que me viene no es una lista de objetos (viene solo uno) lo convierto a una lista de largo 1
    if not isinstance(recording_list, list):
        recording_list = [recording_list, ]

    # Itero y me acomodo los datos en un dict pot ReleaseResourceReference
    for resource_data in recording_list:
        resource_list_by_ref[resource_data['ResourceReference']] = resource_data

    return resource_list_by_ref


def parse_time_string(s):
    s = s.replace('PT','')
    h = s.split("H")[0]
    m = s.split("H")[1].split("M")[0]
    s = s.split("H")[1].split("M")[1].split("S")[0]
    s_time = datetime.time(hour=int(h), minute=int(m), second=int(s))
    return s_time

def get_track_data_joined_by_ref(track_list_by_ref, resource_list_by_ref):
    track_data_for_insert = list()
    for track_data in track_list_by_ref:
        resource_data = resource_list_by_ref[track_data]

        track_data_for_insert.append({
            "name_track": xml_mapper.get_dict_by_language_code_in_list(resource_data['DisplayTitle'])['TitleText'],
            "isrc_track": resource_data['SoundRecordingEdition']['ResourceId']['ISRC'],
            "version_track": "no hay en el xml un subtitulo",
            "length_track": parse_time_string(resource_data['Duration']),
            "explicit_track": 1 if 'explicit' in resource_data['ParentalWarningType'] else 0,
            "active_track": 1,
            "specific_data_track": "JSON_OBJECT('available_128',true,'available_320',true,'available_preview',true)"

        })

    return track_data_for_insert

def upsert_track_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    import pymysql
    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    track_list = get_track_list(release_list)

    resource_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ResourceList'])
    recording_list = get_recording_list(resource_list)

    track_data_for_insert = get_track_data_joined_by_ref(track_list, recording_list)
    logging.info("Se cargaron los datos del xml")

    keys = track_data_for_insert[0].keys()
    values = list()
    for track_data in track_data_for_insert:
        values.append(
            '("{name_track}", "{isrc_track}", "{version_track}",'
            '"{length_track}", "{explicit_track}", "{active_track}", {specific_data_track})'.format(
                name_track=track_data['name_track'].replace('"', '\\"'),
                isrc_track=track_data['isrc_track'],
                version_track=track_data['version_track'],
                length_track=track_data['length_track'],
                explicit_track=track_data['explicit_track'],
                active_track=track_data['active_track'],
                specific_data_track=track_data['specific_data_track'],
                insert_id_message=insert_id_message,
                update_id_message=update_id_message,

            )
        )

    logging.info("Se crearon las tuplas para insertar en la bbdd")
    #
    sql  = ("INSERT INTO feed.tracks ({}) VALUES {}  "
            "ON DUPLICATE KEY UPDATE "
            "name_track = name_track, version_track = version_track, length_track = length_track, "
            "explicit_track = explicit_track, active_track = 1, specific_data_track = specific_data_track, "
            "audi_edited_track = CURRENT_TIMESTAMP, update_id_message={}").format(",".join(keys), ",".join(values), update_id_message)


    logging.info("Se creo la consulta upsert en mysql: {}".format(sql))

    query_values = {}
    rows = connections.execute_query(db_pool, sql, query_values)
    logging.info("Se ejecutó la consulta upsert en mysql")

    # upsert en mongo
    # sql_select = xml_mapper.get_select_of_last_updated_insert_fields(
    #     ("name_track",), "tracks", values
    # )
    # rows = connections.execute_query(db_pool, sql_select, {})
    #
    # # Estos son los nombres de los campos de la tabla label de la base
    # # en mysql y hay que pasarlo al siquiente método.
    # structure = [
    #     "id_track", "isrc_track", "name_track", "version_track", "length_track", "explicit_track", "active_track",
    #     "specific_data_track", "audi_edited_track", "audi_created_track", "update_id_message", "insert_id_message",
    # ]
    # result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'tracks', structure=structure)

    return rows

def upsert_tracks(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_track_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


