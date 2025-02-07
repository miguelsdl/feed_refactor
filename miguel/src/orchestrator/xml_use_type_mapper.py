import logging
from sqlalchemy import text
import connections
import xml_mapper


def upsert_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):

    try:
        deal_term_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealList'])
        logging.info("Se cargaron los datos del xml")

        upsert_query = text("""
        INSERT INTO feed.use_types (
            name_use_type, description_use_type, insert_id_message
        )
        VALUES (
            :name_use_type, :description_use_type, :insert_id_message
        )
        ON DUPLICATE KEY UPDATE
            name_use_type = name_use_type,
            description_use_type = '',
            audi_edited_use_type = CURRENT_TIMESTAMP,
            update_id_message={}
        """.format(update_id_message))

        query_values = []
        for d in deal_term_list['ReleaseDeal']['Deal']:
            for u in d['DealTerms']['UseType']:
                query_values.append({
                    'name_use_type': u,
                    'description_use_type': '',
                    'insert_id_message': insert_id_message,
                })


        connections.execute_query(db_pool, upsert_query, query_values, list_map=True)

        logging.info("Se ejecutó la consulta upsert en mysql")

        # upsert en mongo
        values = list()
        for d in deal_term_list['ReleaseDeal']['Deal']:
            for u in d['DealTerms']['UseType']:
                values.append("('{name}', null, {insert_id_message})".format(name=u, insert_id_message=insert_id_message))

        sql_select = xml_mapper.get_select_of_last_updated_insert_fields(
            ("name_use_type", ), "use_types", values
        )
        rows = connections.execute_query(db_pool, sql_select, {})

        # Estos son los nombres de los campos de la tabla label de la base
        # en mysql y hay que pasarlo al siquiente método.
        structure = [
            "id_use_type", "name_use_type", "description_use_type", "audi_edited_use_type", "audi_created_use_type",
            "update_id_message", "insert_id_message",
        ]
        result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'use_types', structure=structure)
        return True
    except KeyError as e:
        logging.info(f"Error al insertar los datos (genres) en mysql: {e}")
        return None

def upsert_use_type(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


