import datetime
import logging
from sqlalchemy import text
import connections
import xml_mapper


def get_artist_id_from_db(conn, artist_name):
    sql = "SELECT id_artist, name_artist FROM feed.artists WHERE name_artist IN {};"\
           .format(xml_mapper.list_to_sql_in_str(artist_name))
    rows = connections.execute_query(conn, sql, {})

    artist_data = list()
    for row in rows:
        artist_data.append({"artist_id": row[0], "name_artist": row[1]})

    return artist_data

def upsert_rel_album_track_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    rows = list()
    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    album_data = xml_mapper.get_album_data(release_list)
    album = xml_mapper.get_album_id_from_db(db_pool, album_data['upc'])

    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_list_by_ref = xml_mapper.get_party_list(party_list)

    artist_name = dict()
    for a in album_data['artist']:
        artist_name[party_list_by_ref[a]] = a

    artist = get_artist_id_from_db(db_pool, list(artist_name.keys()))

    values = list()
    for a in artist:
        rol = album_data["artist"][artist_name[a['name_artist']]]['DisplayArtistRole']
        values.append(
            '("{id_album}", {id_artist}, "{artist_role_album_artist}", 1, {insert_id_message})'.format(
                id_album=album['album_id'],
                id_artist=a['artist_id'],
                artist_role_album_artist=rol,
                insert_id_message=insert_id_message
            )
        )

    sql = "insert into feed.albums_artists (id_album, id_artist, artist_role_album_artist, active_album_artist, insert_id_message) " \
          "values {} ON DUPLICATE KEY UPDATE active_album_artist = 1,"\
          "audi_edited_album_artist = CURRENT_TIMESTAMP, update_id_message={};"\
          .format(",".join(values), update_id_message)
    rows = connections.execute_query(db_pool, sql, {})

    return rows

def upsert_rel_album_track(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_rel_album_track_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


