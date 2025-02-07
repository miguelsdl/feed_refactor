import logging
import connections
import xml_mapper
from sqlalchemy import text

def get_party_list(party_list):
    party_reference = dict()

    try:
        for party in party_list:
            if isinstance(party['PartyName'], dict):
                # Si viene solo un FullName uso ese
                party_reference[party['PartyReference']] = {"name":[party['PartyName']['FullName'], ]}

                if "PartyId" in party:
                    party_reference[party['PartyReference']]['party_id'] = party['PartyId']

            elif isinstance(party['PartyName'], list):
                # Si viene una lista de FullNames busco cual tengo que usar

                temp_list = list()
                for name in party['PartyName']:
                    temp_list.append(name['FullName'])
                party_reference[party['PartyReference']] = temp_list

                if "PartyId" in party:
                    party_reference[party['PartyReference']]['party_id'] = party['PartyId']


        logging.info("Se leyeron los nombres correctamente desde el PartyList")
        return party_reference
    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None

def get_release_party_reference(release_list):
    release_label_reference_list = set()
    for release in release_list:
        if isinstance(release_list[release], dict):
            release_label_reference_list.add(release_list[release]['ReleaseLabelReference']['#text'])
        elif isinstance(release_list[release], list):
            for o in release_list[release]:
                release_label_reference_list.add(o['ReleaseLabelReference']['#text'])

    return release_label_reference_list

def filter_fullname_by_release_reference_label(fullname_list, release_label_reference_list):
    labels = list()
    for fullname in fullname_list:
        if fullname in release_label_reference_list:
            labels.append(fullname_list[fullname]['name'][0])
    return labels

def get_label_and_release_from_xml(json_dict, ddex_map):

    try:
        party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
        full_names = get_party_list(party_list)

        release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
        reference = get_release_party_reference(release_list)

        labels = filter_fullname_by_release_reference_label(full_names, reference)

        return labels
    except KeyError as e:
        logging.info(f"Error al leer PartyList del xml: {e}")
        return None

def upsert_label(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    try:
        label_from_xml = get_label_and_release_from_xml(json_dict, ddex_map)
        logging.info("Se cargaron los datos del xml")

        values = list()
        for label in label_from_xml:
            values.append("('{name}', true, {insert_id_message})". format(name=label, insert_id_message=insert_id_message))
        logging.info("Se crearon las tuplas para insertar en la bbdd")

        sql  = """INSERT INTO feed.labels (name_label, active_label, insert_id_message) VALUES """ + ",".join(values) + \
               """ON DUPLICATE KEY UPDATE active_label = 1, audi_edited_label = CURRENT_TIMESTAMP, update_id_message={}""".format(update_id_message)
        logging.info("Se creo la consulta upsert en mysql: {}".format(sql))

        upsert_query = text("""
        INSERT INTO feed.labels (
            name_label, active_label, insert_id_message
        )
        VALUES (
            :name_label, :active_label, :insert_id_message
        )
        ON DUPLICATE KEY UPDATE
            name_label = name_label,
            active_label = active_label,
            audi_edited_label = CURRENT_TIMESTAMP,
            update_id_message={}
        """.format(update_id_message))

        query_values = []
        for label in label_from_xml:
            query_values.append({
                'name_label': label,
                'active_label': True,
                'insert_id_message': insert_id_message,
            })

        connections.execute_query(db_pool, upsert_query, query_values)
        logging.info("Se ejecutó la consulta upsert en mysql")





        # upsert en mongo
        sql_select = xml_mapper.get_select_of_last_updated_insert_fields(
            ("name_label", "active_label"), "labels", values
        )
        rows = connections.execute_query(db_pool, sql_select, {})

        # Estos son los nombres de los campos de la tabla label de la base
        # en mysql y hay que pasarlo al siquiente método.
        structure = [
            "id_label", "name_label", "active_label", "audi_edited_label",
            "audi_created_label", "update_id_message", "insert_id_message"
        ]
        result = xml_mapper.update_in_mongo_db2(db_mongo, rows, 'labels', structure=structure)

        return result
    except Exception as e:
        logging.info(f"Error al insertar los datos (labels) en mysql: {e}")
        return None



