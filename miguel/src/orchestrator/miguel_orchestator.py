from importlib import import_module

import xmltodict
import logging
import yaml
from sqlalchemy import text
import os
import connections
import xml_label_mapper
import xml_genre_mapper
import xml_use_type_mapper
import xml_comercial_model_type_mapper
import xml_track_mapper2
import xml_contributor_mapper

# Conectar a MySQL
db_pool = connections.get_db_connection_pool('feed_mysql')
db_mongo = None

# Leer el contenido del archivo
file_path = "/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/A10301A0002913028M.xml"
with open(file_path, 'r', encoding='utf-8') as file:
    file_content = file.read()
# print(file_content)

# paso el archivo xml a dict
json_dict = xmltodict.parse(file_content)
# print(json_dict)

# Cargar el archivo de configuración para la versión DDEX 4.3
with open('/home/miguel/PycharmProjects/feed_refactor/miguel/src/settings/ddex_43.yaml', 'r') as config_file:
    ddex_map = yaml.safe_load(config_file)


# upserted_label = xml_label_mapper.upsert_label(db_mongo, db_pool, json_dict, ddex_map)
#
# upserted_genre = xml_genre_mapper.upsert_genre(db_mongo, db_pool, json_dict, ddex_map)
#
# upserted_use_type = xml_use_type_mapper.upsert_use_type(db_mongo, db_pool, json_dict, ddex_map)
#
# upserted_comercial_model_type = (
#     xml_comercial_model_type_mapper.upsert_commercial_use_type(db_mongo, db_pool, json_dict, ddex_map)
# )

# upserted_tracks = xml_track_mapper2.upsert_tracks(db_mongo, db_pool, json_dict, ddex_map)

xml_contributor_mapper.upsert_contributors(db_mongo, db_pool, json_dict, ddex_map)