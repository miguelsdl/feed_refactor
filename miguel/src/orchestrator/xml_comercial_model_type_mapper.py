import logging
from sqlalchemy import text
import connections
import xml_mapper


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

def upsert_commercial_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    try:
        deal_term_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealTerms'])
        commercial_model_type_list = get_deal_term_list(deal_term_list)
        logging.info("Se cargaron los datos del xml")

        upsert_query = text("""
        INSERT INTO feed.comercial_model_types (
            name_cmt, description_cmt, insert_id_message
        )
        VALUES (
            :name_cmt, :description_cmt, :insert_id_message
        )
        ON DUPLICATE KEY UPDATE
            name_cmt = name_cmt,
            description_cmt = '',
            audi_edited_cmt = CURRENT_TIMESTAMP,
            update_id_message={}
        """.format(update_id_message))

        query_values = []
        for ut in commercial_model_type_list:
            query_values.append({
                'name_cmt': ut,
                'active_genre': True,
                'description_cmt': '',
                'insert_id_message': insert_id_message,
            })


        connections.execute_query(db_pool, upsert_query, query_values, list_map=True)
        logging.info("Se ejecutó la consulta upsert en mysql")
        values = list()
        for ut in commercial_model_type_list:
            values.append("('{name}', null, {insert_id_message})".format(name=ut, insert_id_message=insert_id_message))
        # upsert en mongo
        sql_select = xml_mapper.get_select_of_last_updated_insert_fields(
            ("name_cmt", ), "comercial_model_types", values
        )
        rows = connections.execute_query(db_pool, sql_select, {})

        # Estos son los nombres de los campos de la tabla label de la base
        # en mysql y hay que pasarlo al siquiente método.
        structure = [
            "id_cmt", "name_cmt", "description_cmt", "audi_edited_cmt", "audi_created_cmt", "update_id_message",
            "insert_id_message",
        ]
        result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'comercial_model_types', structure=structure)

        return True
    except KeyError as e:
        logging.info(f"Error al insertar los datos (genres) en mysql: {e}")
        return None

def upsert_commercial_use_type(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_commercial_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


