import logging
from sqlalchemy import text
import connections
import xml_mapper


def get_genre_list(release_list):
    genre_list = set()
    
    try:
        for o in release_list:
            if isinstance(release_list[o], dict):
                genre_list.add(release_list[o]['Genre']['GenreText'])

            elif isinstance(release_list[o], list):

                for g in release_list[o]:
                    genre_list.add(g['Genre']['GenreText'])


        logging.info("Se leyeron los generos correctamente.")
        return genre_list
    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None

# def test_methods(db_mongo, db_pool, json_dict, ddex_map):
#     upsert_genre_in_db(db_mongo, db_pool, json_dict, ddex_map)
    # release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    # get_genre_list(release_list)

def upsert_genre_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):

    try:
        release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
        genre_list = get_genre_list(release_list)
        logging.info("Se cargaron los datos del xml")

        values = list()
        for genre in genre_list:
            values.append("('{name}', true, {insert_id_message})". format(name=genre, insert_id_message=insert_id_message))
        logging.info("Se crearon las tuplas para insertar en la bbdd")

        sql  = text(
            """INSERT INTO feed.genres (name_genre, active_genre, insert_id_message) VALUES """ + ",".join(values) +
            """ON DUPLICATE KEY UPDATE active_genre = 1, audi_edited_genre = CURRENT_TIMESTAMP, update_id_message={}""".format(update_id_message))
        logging.info("Se creo la consulta upsert en mysql: {}".format(sql))

        query_values = {}
        connections.execute_query(db_pool, sql, query_values)
        logging.info("Se ejecutó la consulta upsert en mysql")

        # upsert en mongo
        sql_select = xml_mapper.get_select_of_last_updated_insert_fields(
            ("name_genre", ), "genres", values
        )
        rows = connections.execute_query(db_pool, sql_select, {})

        # Estos son los nombres de los campos de la tabla label de la base
        # en mysql y hay que pasarlo al siquiente método.
        structure = [
            "id_genre", "name_genre", "active_genre", "audi_edited_genre", "audi_created_genre", "update_id_message",
            "insert_id_message"
        ]
        result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'genres', structure=structure)

        # # upsert en mongo
        # sql_select = xml_mapper.get_select_of_last_updated_insert_fields(
        #     ("name_genre", "active_genre"), "genres", values
        # )
        # query_values = {}
        # rows = connections.execute_query(db_pool, sql_select, query_values)
        # result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'genres',)
        # return rows

    except KeyError as e:
        logging.info(f"Error al insertar los datos (genres) en mysql: {e}")
        return None

def upsert_genre(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_genre_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


