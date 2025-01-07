import xmltodict
import logging
import yaml
from sqlalchemy import text
import os

import connections
import xml_album_mapper
import xml_artist_mapper
import xml_track_mapper

# Conectar a MongoDB
db_mongo = connections.get_mongo_client('deliveries')

# Conectar a MySQL
db_pool = connections.get_db_connection_pool('feed_mysql')


# Ruta del archivo en el disco
file_path = "/Users/manuelrodriguez/Documents/Workspace/kuack/feed_refactor_miguel/xml/A10301A0002913028M.xml"  # Cambia esto por la ruta real del archivo

    
# Verificar si el archivo existe
if not os.path.exists(file_path):
    logging.warning(f"El archivo {file_path} no existe.")
    # Aquí puedes realizar alguna acción adicional si el archivo no existe
else:
    # Leer el contenido del archivo
    with open(file_path, 'r', encoding='utf-8') as file:
        file_content = file.read()

    # Verificar si el archivo está vacío
    if not file_content.strip():
        logging.warning(f"El archivo {file_path} está vacío.")
        ern = {"ruta": file_path, "motivo": "Archivo vacío"}  # Ejemplo de registro
        db_mongo['s3_ern_message_notifications_failed'].insert_one(ern)
    else:
        # Convertir el contenido XML a un diccionario JSON
        json_dict = xmltodict.parse(file_content)
        logging.info(f"El archivo {file_path} fue convertido exitosamente a un diccionario JSON.")
        # Aquí puedes procesar el `json_dict` según tus necesidades

# Cargar el archivo de configuración para la versión DDEX 4.3
with open('/Users/manuelrodriguez/Documents/Workspace/kuack/feed_refactor/settings/ddex_43.yaml', 'r') as config_file:
    ddex_map = yaml.safe_load(config_file)




########################################################################################################
# Procesamiento del XML e inserción o actualización de Entidades en Catalogos MySQL y MongoDB
########################################################################################################

# Insertar o actualizar el álbum en la base de datos
upserted_album = xml_album_mapper.upsert_album(db_mongo, db_pool, json_dict, ddex_map)
logging.info(f"Álbum actualizado o insertado: {upserted_album}")

# Insertar o actualizar el artista en la base de datos
upserted_artists = xml_artist_mapper.upsert_artist(db_mongo, db_pool, json_dict, ddex_map, upserted_album)
logging.info(f"Artistas actualizados o insertados: {upserted_artists}")

# Insertar o actualizar el track en la base de datos    
upserted_tracks = xml_track_mapper.upsert_tracks(db_mongo, db_pool, json_dict, ddex_map, upserted_album)
logging.info(f"Tracks actualizados o insertados: {upserted_tracks}")


