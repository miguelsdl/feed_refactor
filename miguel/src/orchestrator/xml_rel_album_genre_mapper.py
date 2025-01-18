import logging
import connections
import xml_mapper


def get_genre_id_from_db(conn, genre):
    sql = "SELECT id_genre, name_genre FROM feed.genres WHERE name_genre = '{}';".format(genre['GenreText'])
    rows = connections.execute_query(conn, sql, {})
    genre_data = {"id_genre": rows[0][0], "name_genre": rows[0][1]}

    return genre_data

def upsert_rel_album_genre_in_db(db_mongo, db_pool, json_dict, ddex_map):
    rows = list()
    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    album_data = xml_mapper.get_album_data(release_list)

    album = xml_mapper.get_album_id_from_db(db_pool, album_data['upc'])
    genre = get_genre_id_from_db(db_pool, album_data['genre'])

    sql = "insert into albums_genres (id_album, id_genre) values ({}, {}) ON DUPLICATE KEY UPDATE " \
           "audi_edited_album_genre = CURRENT_TIMESTAMP;".format(album['album_id'], genre['id_genre'])
    rows = connections.execute_query(db_pool, sql, {})

    return rows

def upsert_rel_album_genre(db_mongo, db_pool, json_dict, ddex_map):
    upsert_rel_album_genre_in_db(db_mongo, db_pool, json_dict, ddex_map)


