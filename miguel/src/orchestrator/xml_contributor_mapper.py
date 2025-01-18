import datetime
import logging
from sqlalchemy import text
import connections
import xml_mapper

# consulta sql para agrega una constrain para que funcione ON DUPLICATE
# ALTER TABLE feed.contributors ADD CONSTRAINT constr_contributors  UNIQUE (name_contri(100));

def get_contributor_list(resource_list):
    contributor_list_by_ref = dict()
    sound_rec_list = resource_list['SoundRecording']

    # si lo que me viene no es una lista de objetos (viene solo uno) lo convierto a una lista de largo 1
    if not isinstance(sound_rec_list, list):
        sound_rec_list = [sound_rec_list, ]

    for record in sound_rec_list:
        contributor_list = record['Contributor'] if 'Contributor' in record else []
        if not isinstance(contributor_list, list):
            contributor_list = [contributor_list, ]
        for o in contributor_list:
            contributor_list_by_ref[o['ContributorPartyReference']] = o

    return contributor_list_by_ref

def get_party_list(party_list):
    artist_list_by_ref = dict()

    # si lo que me viene no es una lista de objetos (viene solo uno) lo convierto a una lista de largo 1
    if not isinstance(party_list, list):
        party_list = [party_list, ]

    # Itero y me acomodo los datos en un dict pot ReleaseResourceReference
    for artist in party_list:
        artist_list_by_ref[artist['PartyReference']] = artist

    return artist_list_by_ref

def get_party_and_contributors_data_joined(contributor_list_by_ref, artist_list_by_ref):
    contributor_data_for_insert = list()
    for contributor_data in contributor_list_by_ref:
        if isinstance(artist_list_by_ref[contributor_data]['PartyName'], list):
            name_contri = artist_list_by_ref[contributor_data]['PartyName'][0]['FullName']
        else:
            name_contri = artist_list_by_ref[contributor_data]['PartyName']['FullName']
        contributor_data_for_insert.append({
            "name_contri": name_contri,
            "active_contri": 1,
        })

    return contributor_data_for_insert

def upsert_contributors_in_db(db_mongo, db_pool, json_dict, ddex_map):
    resource_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ResourceList'])
    contributor_list_by_ref = get_contributor_list(resource_list)

    if len(contributor_list_by_ref):

        party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
        artist_list_by_ref = get_party_list(party_list)

        contri_data_for_insert = get_party_and_contributors_data_joined(contributor_list_by_ref, artist_list_by_ref)
        logging.info("Se cargaron los datos del xml")

        keys = contri_data_for_insert[0].keys()
        values = list()
        for contri_data in contri_data_for_insert:
            values.append(
                '("{name_contri}", "{active_contri}")'.format(
                    name_contri=contri_data['name_contri'].replace('"', '\\"'),
                    active_contri=contri_data['active_contri'],
                )
            )

        logging.info("Se crearon las tuplas para insertar en la bbdd")
        #
        sql  = text(
            """INSERT INTO feed.contributors ({}) VALUES """.format(",".join(keys))
            + ",".join(values)
            + """ ON DUPLICATE KEY UPDATE name_contri = name_contri, active_contri = 1,
                    audi_edited_contri = CURRENT_TIMESTAMP"""
        )

        logging.info("Se creo la consulta upsert en mysql: {}".format(sql))

        query_values = {}
        rows = connections.execute_query(db_pool, sql, query_values)
        logging.info("Se ejecutó la consulta upsert en mysql")
        return rows
    else:
        return None

def upsert_contributors(db_mongo, db_pool, json_dict, ddex_map):
    upsert_contributors_in_db(db_mongo, db_pool, json_dict, ddex_map)


