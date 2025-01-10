import logging
from sqlalchemy import text
import connections
import xml_mapper


def get_fullname_from_party_list(party_list):
    fullname_list = []

    try:
        for party in party_list:
            if isinstance(party['PartyName'], dict):
                # Si viene solo un FullName uso ese
                fullname_list.append(party['PartyName']['FullName'])
            elif isinstance(party['PartyName'], list):
                # Si viene una lista de FullNames busco cual tengo que usar
                fullname_list.append(party['PartyName'][0]['FullName'])
        logging.info("Se leyeron los nombres correctamente desde el PartyList")
        return fullname_list
    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None


def get_label_from_xml(json_dict, ddex_map):

    try:
        party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
        labels = get_fullname_from_party_list(party_list)

        return labels
    except KeyError as e:
        logging.info(f"Error al leer PartyList del xml: {e}")
        return None

def upsert_label(db_mongo, db_pool, json_dict, ddex_map):

    try:
        label_from_xml = get_label_from_xml(json_dict, ddex_map)
        logging.info("Se cargaron los datos del xml")

        values = list()
        for label in label_from_xml:
            values.append("('{name}', true)". format(name=label))
        logging.info("Se crearon las tuplas para insertar en la bbdd")

        sql  = text(
            """INSERT INTO feed.labels (name_label, active_label) VALUES """ + ",".join(values) +
            """ON DUPLICATE KEY UPDATE active_label = 1, audi_edited_label = CURRENT_TIMESTAMP""")
        print(sql)
        logging.info("Se creo la consulta upsert en mysql: {}".format(sql))

        query_values = {}
        connections.execute_query(db_pool, sql, query_values)
        logging.info("Se ejecutó la consulta upsert en mysql")
        return True
    except KeyError as e:
        logging.info(f"Error al insertar los datos (labels) en mysql: {e}")
        return None



