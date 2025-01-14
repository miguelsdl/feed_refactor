import logging
from sqlalchemy import text
import connections
import xml_mapper

# consulta sql para agrega una constrain para que funcione ON DUPLICATE
# ALTER TABLE feed.use_types ADD CONSTRAINT constr_name_use_type  UNIQUE (name_use_type(100));

def get_deal_term_list(deal_term_list):
    #  TODO - ver si se puede optimizar este método
    use_type_list = set()
    
    try:
        for o in deal_term_list:
            if isinstance(deal_term_list, dict):
                pass

            elif isinstance(deal_term_list, list):
                for dt in deal_term_list:
                    for ut in dt:
                        use_type_list.update(set(dt[ut]['UseType']))

        logging.info("Se leyeron los generos correctamente.")
        return use_type_list
    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None


def upsert_use_type_in_db(db_mongo, db_pool, json_dict, ddex_map):

    try:
        deal_term_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealTerms'])
        use_type_list = get_deal_term_list(deal_term_list)
        logging.info("Se cargaron los datos del xml")

        values = list()
        for ut in use_type_list:
            values.append("('{name}', null)". format(name=ut))
        logging.info("Se crearon las tuplas para insertar en la bbdd")

        sql  = text(
            """INSERT INTO feed.use_types (name_use_type, description_use_type) VALUES """ + ",".join(values) +
            """ON DUPLICATE KEY UPDATE audi_edited_use_type = CURRENT_TIMESTAMP""")

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


