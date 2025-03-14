import logging
from sqlalchemy import text, bindparam
import connections
import xml_mapper


def get_sound_recording_list(json_dict, ddex_map):
    sound_recording = xml_mapper.get_value_from_path(json_dict, ddex_map['SoundRecording'])
    if not isinstance(sound_recording, list):
        sound_recording = [sound_recording, ]

    return sound_recording

def get_isrc(sound_recording_item):
    isrc = sound_recording_item['SoundRecordingEdition']['ResourceId']['ISRC']

    return isrc

def get_track_id_isrc_map(isrc_track, db_pool):
    sql_select = text("""SELECT id_track, isrc_track FROM feed.tracks WHERE isrc_track IN :isrc_track""").bindparams(
        bindparam("isrc_track", value=isrc_track, expanding=True)
    )
    rows = connections.execute_query(db_pool, sql_select, isrc_track, list_map=True)
    # Devuelve una lista así: [(11, 'USSM10606175'), (12, 'USSM18200435'), (2,

    track_id_isrc_map = dict()
    for row in rows:
        track_id_isrc_map.setdefault(row[1].lower(), row[0])

    return track_id_isrc_map

def get_contributor_id_name_map(name_contri, db_pool):
    sql_select = (text("""SELECT id_contri, name_contri FROM feed.contributors WHERE name_contri IN :name_contri""")
    .bindparams(
        bindparam("name_contri", value=name_contri, expanding=True)
    ))
    rows = connections.execute_query(db_pool, sql_select, name_contri, list_map=True)
    # Devuelve una lista así: [(11, 'Juan Odone'), (12, 'Carlos Sheim'), (2,

    contributor_name_id_map = dict()
    for row in rows:
        contributor_name_id_map.setdefault(row[1].lower(), row[0])

    return contributor_name_id_map


def get_contributor_list(sound_recording_item):
    if 'Contributor' in sound_recording_item:
        contributor_list = sound_recording_item['Contributor']
    else:
        contributor_list = list()

    if not isinstance(contributor_list, list):
        contributor_list = [contributor_list, ]

    return contributor_list

def get_role_list(contributor):
    role_list = contributor['Role']
    if not isinstance(role_list, list):
        role_list = [role_list, ]

    role_final_list = []
    for role in role_list:
        if "#text" in role:
            if role["#text"] == 'UserDefined':
                role_final_list.append(role['@UserDefinedValue'])
        elif isinstance(role, str):
            role_final_list.append(role)
        else:
            raise Exception("No tiene que pasar por acá")

    return role_final_list

def upsert_use_track_contributor_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_name_by_ref = xml_mapper.get_party_list(party_list)

    sound_recording_list = get_sound_recording_list(json_dict, ddex_map)
    isrc_list = []
    contributor_name_list = []

    for sr in sound_recording_list:
        isrc_list.append(get_isrc(sr))

        for contributor in get_contributor_list(sr):
            contributor_name_list.append(party_name_by_ref[contributor['ContributorPartyReference']])

    if contributor_name_list:

        track_id_isrc_map = get_track_id_isrc_map(isrc_list, db_pool)
        contributor_id_name_map = get_contributor_id_name_map(contributor_name_list, db_pool)

        query_values = []
        # Itero sobre los tracks
        for sr in sound_recording_list:
            id_track = track_id_isrc_map[get_isrc(sr).lower()]
            # itero sobre los contributors de cada track
            for contributor in get_contributor_list(sr):
                contributor_reference = contributor['ContributorPartyReference']
                contributor_name = party_name_by_ref[contributor_reference]
                contributor_id = contributor_id_name_map[contributor_name.lower()]
                # itero sobre los roles de cada contributor (de cada track)
                for role in get_role_list(contributor):
                    query_values.append({
                        "id_track": id_track,
                        "id_contri": contributor_id,
                        "contributor_role_track_contri": "{}".format(role),
                        "insert_id_message": insert_id_message,
                        "update_id_message": update_id_message
                    })

            upsert_query = text("""
            INSERT INTO feed.tracks_contributors (
                id_track, id_contri, contributor_role_track_contri, insert_id_message, update_id_message, audi_edited_track_contri
            )
            VALUES (
                :id_track, :id_contri, :contributor_role_track_contri, :insert_id_message, :update_id_message, CURRENT_TIMESTAMP
            )
            ON DUPLICATE KEY UPDATE
                audi_edited_track_contri = CURRENT_TIMESTAMP,
                id_track = id_track,
                id_contri = id_contri,
                contributor_role_track_contri = contributor_role_track_contri,
                update_id_message={}
            """.format(update_id_message))
            connections.execute_query(db_pool, upsert_query, query_values, list_map=True)
            logging.info("Se ejecutó la consulta upsert en mysql")

        return_value = 0
    else:
        logging.info("En este archivo no hay Contributors")
        return_value = 0

    return return_value

def upsert_track_contributor(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_use_track_contributor_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
