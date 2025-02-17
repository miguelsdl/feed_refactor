from sqlalchemy import text
import connections
import xml_mapper


def get_genre_id_from_db(conn, genre):
    sql = "SELECT id_genre, name_genre FROM feed.genres WHERE name_genre = '{}';".format(genre['GenreText'])
    rows = connections.execute_query(conn, sql, {})
    genre_data = {"id_genre": rows[0][0], "name_genre": rows[0][1]}

    return genre_data

def upsert_rel_album_genre_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    rows = list()
    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    album_data = xml_mapper.get_album_data(release_list)

    album = xml_mapper.get_album_id_from_db(db_pool, album_data['upc'])
    genre = get_genre_id_from_db(db_pool, album_data['genre'])

    # sql = "insert into albums_genres (id_album, id_genre, insert_id_message) values ({}, {}, {}) ON DUPLICATE KEY UPDATE " \
    #        "audi_edited_album_genre = CURRENT_TIMESTAMP, update_id_message={};".format(album['album_id'], genre['id_genre'], insert_id_message, update_id_message)
    # rows = connections.execute_query(db_pool, sql, {})

    upsert_query = text("""
    INSERT INTO feed.albums_genres (
        id_album, id_genre, insert_id_message, audi_edited_album_genre, update_id_message
    )
    VALUES (
        :id_album, :id_genre, :insert_id_message, CURRENT_TIMESTAMP, :update_id_message
    )
    ON DUPLICATE KEY UPDATE
        id_album = id_album,
        id_genre = id_genre,
        audi_edited_album_genre = CURRENT_TIMESTAMP,
        update_id_message={}
    """.format(update_id_message))

    query_values = {"id_album": album['album_id'], "id_genre": genre['id_genre'], "insert_id_message": insert_id_message, "update_id_message": update_id_message}

    connections.execute_query(db_pool, upsert_query, query_values)

    return True

def upsert_rel_album_genre(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_rel_album_genre_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


