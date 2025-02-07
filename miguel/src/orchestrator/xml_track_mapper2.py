import datetime
import json
import logging
from sqlalchemy import text
import connections
import xml_mapper
import re


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

        try:
            version_track = xml_mapper.get_dict_by_language_code_in_list(resource_data['DisplayTitle'])['SubTitle']["#text"]
        except Exception as e:
            version_track = ''
            logging.error("Error, no hay subtitulo en el xml: " + str(e))

        track_data_for_insert.append({
            "name_track": xml_mapper.get_dict_by_language_code_in_list(resource_data['DisplayTitle'])['TitleText'],
            "isrc_track": resource_data['SoundRecordingEdition']['ResourceId']['ISRC'],
            "version_track": version_track,
            "length_track": parse_time_string(resource_data['Duration']),
            "explicit_track": 1 if 'explicit' in resource_data['ParentalWarningType'] else 0,
            "active_track": 1,
            "specific_data_track": "'available_128',true,'available_320',true,'available_preview',true"

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

    upsert_query = text("""
    INSERT INTO feed.tracks (
        name_track, isrc_track, version_track, length_track, explicit_track,
        active_track, specific_data_track, insert_id_message, audi_edited_track, update_id_message
    )
    VALUES (
        :name_track, :isrc_track, :version_track, :length_track, :explicit_track, 
        :active_track, :specific_data_track, :insert_id_message, CURRENT_TIMESTAMP, :update_id_message
    )
    ON DUPLICATE KEY UPDATE
        name_track = name_track, 
        version_track = version_track, 
        length_track = length_track, 
        explicit_track = explicit_track, 
        active_track = active_track, 
        specific_data_track = specific_data_track, 
        update_id_message = {},
        audi_edited_track = CURRENT_TIMESTAMP

    """.format(update_id_message))

    query_values = []
    for track_data in track_data_for_insert:
        a = track_data['specific_data_track']
        specific_data_track = {}
        for i in range(0, len(a), 2):
            key = a[i].replace("'", "")
            val = a[i + 1].replace("'", "")
            specific_data_track[key] = True if val == "true" else val

        query_values.append({
            "name_track": track_data['name_track'],
            "isrc_track": track_data['isrc_track'],
            "version_track": track_data['version_track'],
            "length_track": track_data['length_track'],
            "explicit_track": track_data['explicit_track'],
            "active_track": track_data['active_track'],
            "specific_data_track": json.dumps({'available_128': True, 'available_320':True, 'available_preview': True}),
            "insert_id_message": insert_id_message,
            "update_id_message": update_id_message
        })

    values = list()
    values_isrc_track = list()
    for track_data in track_data_for_insert:
        values_isrc_track.append(track_data['isrc_track'])
        values.append(
            '("{name_track}", "{isrc_track}", "{version_track}",'
            '"{length_track}", "{explicit_track}", "{active_track}", {specific_data_track})'.format(
                name_track=track_data['name_track'].replace('"', '\\"'),
                isrc_track=track_data['isrc_track'],
                version_track=track_data['version_track'],
                # length_track=track_data['length_track'],
                length_track= '00:00:00', #if track_data['length_track'].second == 0 and track_data['length_track'].minute == 0 else track_data['length_track'],
                explicit_track=track_data['explicit_track'],
                active_track=track_data['active_track'],
                specific_data_track=track_data['specific_data_track'],

            )
        )

    connections.execute_query(db_pool, upsert_query, query_values, list_map=True)
    logging.info("Se ejecutó la consulta upsert en mysql")

    # upsert en mongo
    sql_in = "('" + "','".join(values_isrc_track) + "')"
    sql_select = "SELECT * FROM feed.tracks WHERE isrc_track IN {}".format(sql_in)
    rows = connections.execute_query(db_pool, sql_select, {})

    # Estos son los nombres de los campos de la tabla label de la base
    # en mysql y hay que pasarlo al siquiente método.
    structure = [
        "id_track", "isrc_track", "name_track", "version_track", "length_track", "explicit_track", "active_track",
        "specific_data_track", "audi_edited_track", "audi_created_track", "update_id_message", "insert_id_message",
    ]
    result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'tracks', structure=structure)

    return result

def upsert_tracks(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_track_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


