import logging
from sqlalchemy import text
import connections
import xml_mapper

# consulta sql para agrega una constrain para que funcione ON DUPLICATE
# ALTER TABLE feed.use_types ADD CONSTRAINT constr_name_use_type  UNIQUE (name_use_type(100));


def upsert_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map):

    try:
        deal_term_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealList'])
        logging.info("Se cargaron los datos del xml")

        values = list()
        for d in deal_term_list['ReleaseDeal']['Deal']:
            for u in d['DealTerms']['UseType']:
                values.append("('{name}', null)".format(name=u))


        sql  = """INSERT INTO feed.use_types (name_use_type, description_use_type) VALUES """ + ",".join(values) + \
               """ON DUPLICATE KEY UPDATE audi_edited_use_type = CURRENT_TIMESTAMP"""

        logging.info("Se creo la consulta upsert en mysql: {}".format(sql))

        query_values = {}
        connections.execute_query(db_pool, sql, query_values)
        logging.info("Se ejecutó la consulta upsert en mysql")
        return True
    except KeyError as e:
        logging.info(f"Error al insertar los datos (genres) en mysql: {e}")
        return None

def upsert_use_type(db_mongo, db_pool, json_dict, ddex_map):
    upsert_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map)


