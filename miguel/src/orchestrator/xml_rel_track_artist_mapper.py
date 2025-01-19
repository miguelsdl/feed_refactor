import logging
import connections
import xml_mapper


def get_tack_data(track_release):
    track_data = dict()
    for t in track_release:
        track_isrc = t['ReleaseId']['ProprietaryId']['#text']
        track_data[track_isrc] = t

    return track_data

def get_resource_list_data(resource_list):
    return xml_mapper.get_resource_list_data_by_resource_reference(resource_list)

def get_artist_list_from_recording_list_by_reference(resource_data, reference):
    return 0

# def get_release_data(track_list):
#     track_list_data = dict()
#
#     for r in track_list:
#         r
#
#     return track_list_data

def get_tack_data_from_db(conn, track_data_list):
    db_track_data = dict()
    sql_in = "('" + "','".join(track_data_list.keys()) + "')"
    sql = "SELECT id_track, isrc_track FROM feed.tracks WHERE isrc_track IN {};".format(sql_in)
    rows = connections.execute_query(conn, sql, {})
    for r in rows:
        db_track_data[r[1]] = r[0]

    return db_track_data

def upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map):
    rows = list()
    track_release = xml_mapper.get_value_from_path(json_dict, ddex_map['TrackRelease'])
    track_list_data = get_tack_data(track_release)
    print(0)



    # tracks = get_tack_data_from_db(db_pool, release_data)

    #

    #
    # release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    # album_data = xml_mapper.get_album_data(release_list)
    # album = xml_mapper.get_album_id_from_db(db_pool, album_data['upc'])
    #
    # sql_values = list()
    # for t in tracks:
    #     sql_values.append('({}, {})'.format(album['album_id'], tracks[t]))
    #
    # sql = "insert into albums_tracks (id_album, id_track) values {} ON DUPLICATE KEY UPDATE " \
    #        "audi_edited_album_track = CURRENT_TIMESTAMP;".format(",".join(sql_values))
    # rows = connections.execute_query(db_pool, sql, {})

    return rows

def upsert_rel_track_artist(db_mongo, db_pool, json_dict, ddex_map):
    upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map)


