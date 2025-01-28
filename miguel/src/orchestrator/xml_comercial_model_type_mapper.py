import logging
from sqlalchemy import text
import connections
import xml_mapper

# consulta sql para agrega una constrain para que funcione ON DUPLICATE
# ALTER TABLE feed.comercial_model_types ADD CONSTRAINT cmt_name_use_type  UNIQUE (name_cmt(100));

def get_deal_term_list(deal_term_list):
    #  TODO - ver si se puede optimizar este método
    commercial_model_type_list = set()

    try:
        for o in deal_term_list:
            if isinstance(deal_term_list, dict):
                pass

            elif isinstance(deal_term_list, list):
                for dt in deal_term_list:
                    for ut in dt:
                        commercial_model_type_list.add(dt[ut]['CommercialModelType'])

        logging.info("Se leyeron los generos correctamente.")
        return commercial_model_type_list
    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None


def test_methods(db_mongo, db_pool, json_dict, ddex_map):
    upsert_commercial_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map)
    # deal_term_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealTerms'])
    # a = get_deal_term_list(deal_term_list)

def upsert_commercial_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    try:
        deal_term_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealTerms'])
        commercial_model_type_list = get_deal_term_list(deal_term_list)
        logging.info("Se cargaron los datos del xml")

        values = list()
        for ut in commercial_model_type_list:
            values.append("('{name}', null, {insert_id_message})".format(name=ut, insert_id_message=insert_id_message))
        logging.info("Se crearon las tuplas para insertar en la bbdd")

        sql = text(
            """INSERT INTO feed.comercial_model_types (name_cmt, description_cmt, insert_id_message) VALUES """ + ",".join(values) +
            """ON DUPLICATE KEY UPDATE audi_edited_cmt = CURRENT_TIMESTAMP, update_id_message={}""".format(update_id_message))
        logging.info("Se creo la consulta upsert en mysql: {}".format(sql))

        query_values = {}
        rows = connections.execute_query(db_pool, sql, query_values)
        logging.info("Se ejecutó la consulta upsert en mysql")

        # upsert en mongo
        # sql_select = xml_mapper.get_select_of_last_updated_insert_fields(
        #     ("name_cmt", ), "comercial_model_types", values
        # )
        # rows = connections.execute_query(db_pool, sql_select, {})
        #
        # # Estos son los nombres de los campos de la tabla label de la base
        # # en mysql y hay que pasarlo al siquiente método.
        # structure = [
        #     "id_cmt", "name_cmt", "description_cmt", "audi_edited_cmt", "audi_created_cmt", "update_id_message",
        #     "insert_id_message",
        # ]
        # result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'comercial_model_types', structure=structure)

        return True
    except KeyError as e:
        logging.info(f"Error al insertar los datos (genres) en mysql: {e}")
        return None


def upsert_commercial_use_type(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_commercial_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


