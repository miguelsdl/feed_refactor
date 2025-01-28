import logging

from sqlalchemy.sql.coercions import expect

import connections
import xml_mapper


def get_tack_data(track_release):
    track_data = dict()
    if isinstance(track_release, dict):
        track_release = [track_release, ]
    for t in track_release:
        try:
            track_isrc = t['ReleaseId']['ProprietaryId']['#text']
            track_data[track_isrc] = t
        except Exception as e:
            logging.info(e)
            raise e


    return track_data

def get_tack_data_from_db(conn, track_data_list):
    db_track_data = dict()
    sql_in = "('" + "','".join(track_data_list.keys()) + "')"
    sql = "SELECT id_track, isrc_track FROM feed.tracks WHERE isrc_track IN {};".format(sql_in)
    rows = connections.execute_query(conn, sql, {})
    for r in rows:
        db_track_data[r[1]] = r[0]

    return db_track_data

def upsert_rel_album_track_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    rows = list()
    track_release = xml_mapper.get_value_from_path(json_dict, ddex_map['TrackRelease'])
    track_data = get_tack_data(track_release)
    tracks = get_tack_data_from_db(db_pool, track_data)

    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    album_data = xml_mapper.get_album_data(release_list)
    album = xml_mapper.get_album_id_from_db(db_pool, album_data['upc'])

    sql_values = list()
    for t in tracks:
        sql_values.append('({}, {}, {})'.format(album['album_id'], tracks[t], insert_id_message))

    sql = "insert into albums_tracks (id_album, id_track, insert_id_message) values {} ON DUPLICATE KEY UPDATE " \
           "audi_edited_album_track = CURRENT_TIMESTAMP, update_id_message={};".format(",".join(sql_values), update_id_message)
    rows = connections.execute_query(db_pool, sql, {})

    return rows

def upsert_rel_album_track(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_rel_album_track_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


