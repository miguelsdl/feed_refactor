import logging
import json
from bson import json_util
from sqlalchemy import text

import xml_mapper
import connections


########################################################################################################################
# Funciones de obtención de valores en el archivo DDEX
########################################################################################################################

def get_artist_name(party):
    """
    Obtiene el nombre del artista desde el diccionario de la PartyList.
    
    :param party: Diccionario que representa un artista dentro de PartyList.
    :return: Nombre del artista, o 'Desconocido' si no se encuentra.
    """
    try:
        # Obtener el nodo PartyName, que puede ser una lista o un diccionario
        party_name = party.get('PartyName')

        # Si PartyName es una lista, iteramos para obtener la versión sin lenguaje o la primera
        if isinstance(party_name, list):
            for name in party_name:
                if '@LanguageAndScriptCode' not in name:
                    return name.get('FullName', 'Desconocido')  # Prioridad a los nombres sin idioma especificado
            return party_name[0].get('FullName', 'Desconocido')  # En caso de no encontrar, retornamos el primero
        elif isinstance(party_name, dict):
            return party_name.get('FullName', 'Desconocido')
        else:
            return 'Desconocido'
    
    except KeyError as e:
        logging.warning(f"Clave no encontrada para PartyName: {e}")
        return 'Desconocido'
    

def get_DisplayArtist(release, resources, party_list):
    """
    Obtiene la información de los artistas desde el campo 'DisplayArtist' y mapea su nombre desde 'PartyList'.
    
    :param release: Diccionario que contiene los datos del 'release', incluido 'DisplayArtist'.
    :param party_list: Lista de diccionarios que contiene los datos de los artistas en 'PartyList'.
    :return: Lista de diccionarios con información de los artistas y sus roles.
    """
    try:
        # Obtener el campo DisplayArtist dentro del release
        display_artist_list = release.get('DisplayArtist', [])

        # Si DisplayArtist no es una lista, lo convertimos en una lista
        if isinstance(display_artist_list, dict):
            display_artist_list = [display_artist_list, ]

        # agrego los artist que vienen en resources list
        for sr in resources:
            artist_list = sr['DisplayArtist']
            if not isinstance(artist_list, list):
                artist_list = [artist_list, ]
            for a in artist_list:
                if a not in display_artist_list:
                    display_artist_list.append(a)

        artists = []

        # Iterar sobre cada DisplayArtist en el release
        for display_artist in display_artist_list:
            artist_party_reference = display_artist.get('ArtistPartyReference')

            # Buscar el nombre del artista en PartyList usando ArtistPartyReference
            artist_name = None
            for party in party_list:
                if party.get('PartyReference') == artist_party_reference:
                    artist_name = get_artist_name(party)  # Usar la función get_artist_name para obtener el nombre
                    break

            if artist_name:
                artist_info = {
                    'name_artist': artist_name,
                    'artist_role_album_artist': display_artist.get('DisplayArtistRole', 'Unknown'),
                    'sequence_number': display_artist.get('@SequenceNumber', 'Unknown'),
                    'active_artist': 1
                }
                artists.append(artist_info)

        return artists

    except KeyError as e:
        logging.error(f"Error al obtener la lista de artistas desde DisplayArtist: {e}")
        return []
    

def get_artist_from_xml(json_dict, ddex_map):
    """
    Extrae la información del artista desde el XML mapeado.
    
    :param json_dict: Diccionario JSON con los datos del XML.
    :param ddex_map: Mapeo de las rutas del DDEX definidas en el archivo ddex_43.yaml.
    :return: Lista de diccionarios con información de los artistas.
    """
    try:
        # Obtener la lista de artistas desde el PartyList
        party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
        release = xml_mapper.get_value_from_path(json_dict, ddex_map['Release'])
        resources = xml_mapper.get_value_from_path(json_dict, ddex_map['SoundRecording'])

        # Comprobar si party_list es una lista, si no lo es, convertirlo en una lista para manejar múltiples artistas
        if isinstance(party_list, dict):
            party_list = [party_list, ]  # Si solo hay un artista, lo convertimos en una lista de un elemento

        if isinstance(resources, dict):
            resources = [resources, ]  # Si solo hay un artista, lo convertimos en una lista de un elemento


        # Obtener la información de DisplayArtist y mapear a PartyList
        artists = get_DisplayArtist(release, resources, party_list)
        
        return artists
    
    except KeyError as e:
        logging.error(f"Error al obtener la lista de artistas: {e}")
        return []


def get_artist_from_db(db_pool, name_artist):
    """
    Recupera un artista desde la base de datos MySQL usando su ID.
    
    :param db_pool: Pool de conexiones a la base de datos.
    :param name_artist: Identificador del artista a buscar.
    :return: Diccionario con los datos del artista o None si no se encuentra.
    """
    try:
        select_query = text("SELECT * FROM feed.artists WHERE name_artist = :name_artist")
        query_values = {'name_artist': name_artist}
        artist_rows = connections.execute_query(db_pool, select_query, query_values)

        if artist_rows:
            artist_dict = dict(artist_rows[0]._mapping.items())
            return artist_dict
        else:
            return None
    
    except Exception as e:
        logging.error(f"Error al obtener el artista desde la base de datos: {e}")
        return None
    

def get_album_artist_from_db(db_pool, id_album, id_artist, artist_role_album_artist):
    """
    Recupera un artista desde la base de datos MySQL usando su ID.
    
    :param db_pool: Pool de conexiones a la base de datos.
    :param name_artist: Identificador del artista a buscar.
    :return: Diccionario con los datos del artista o None si no se encuentra.
    """
    try:
        select_query = text("SELECT * FROM feed.albums_artists WHERE id_album = :id_album and id_artist = :id_artist and artist_role_album_artist = :artist_role_album_artist")
        query_values = {'id_album': id_album, 'id_artist': id_artist, 'artist_role_album_artist': artist_role_album_artist}
        albums_artist_rows = connections.execute_query(db_pool, select_query, query_values)

        if albums_artist_rows:
            albums_artist_dict = dict(albums_artist_rows[0]._mapping.items())
            return albums_artist_dict
        else:
            return None
    
    except Exception as e:
        logging.error(f"Error al obtener el artista desde la base de datos: {e}")
        return None    



########################################################################################################################
# Inserción y actualización en MySQL
########################################################################################################################

def upsert_artist_in_db(db_pool, artist_from_xml, album, update_id_message, insert_id_message):
    """
    Inserta o actualiza un artista en la base de datos MySQL usando ON DUPLICATE KEY UPDATE.
    
    :param db_pool: Pool de conexiones a la base de datos.
    :param artist_from_xml: Diccionario con los datos del artista obtenidos del XML.
    :return: Artista insertado o actualizado.
    """
    try:
   
        # Chequea la existencia del artista en la base de datos Mysql
        artist_from_db = get_artist_from_db(db_pool, artist_from_xml.get('name_artist'))

        # Evaluar si el álbum ya existe en la base de datos
        if artist_from_db:
            # artist_from_db es una lista de artistas, obtener el primero
            artist_db_dict = artist_from_db[0] if isinstance(artist_from_db, list) and artist_from_db else artist_from_db
           
            # Si existe, asignar el 'id_album' desde la base de datos al álbum obtenido del XML
            if artist_db_dict and artist_db_dict.get('name_artist') == artist_from_xml.get('name_artist'):
                artist_from_xml['id_artist'] = artist_db_dict.get('id_artist')


        # Construir la consulta SQL con parámetros bind
        upsert_query = text("""
        INSERT INTO feed.artists (
            id_artist, name_artist, id_parent_artist, active_artist, specific_data_artist, insert_id_message, update_id_message
        ) 
        VALUES (
            :id_artist, :name_artist, :id_parent_artist, :active_artist, :specific_data_artist, :insert_id_message, :update_id_message
        )
        ON DUPLICATE KEY UPDATE
            active_artist = VALUES(active_artist),
            update_id_message = VALUES(update_id_message),
            audi_edited_artist = CURRENT_TIMESTAMP
        """)

        # Preparar los valores para la consulta
        query_values = {
            'id_artist': artist_from_xml.get('id_artist'),  # El id del artista es opcional; MySQL lo auto incrementará si es necesario
            'name_artist': artist_from_xml.get('name_artist', None),
            'id_parent_artist': None,  # Se puede agregar lógica para definir esto, si aplica
            'active_artist': artist_from_xml.get('active_artist', 1),  # Se puede cambiar si hay una lógica para definir si el artista está activo o no
            'specific_data_artist': json.dumps(artist_from_xml.get('specific_data_artist', {"cms_image": False})),
            'insert_id_message': insert_id_message,
            'update_id_message': update_id_message
        }

        # Ejecutar la consulta con la función connections.execute_query
        connections.execute_query(db_pool, upsert_query, query_values)
        
        # Recuperar el artista actualizado desde la base de datos
        artist_upserted = get_artist_from_db(db_pool, artist_from_xml.get('name_artist'))

        return artist_upserted

    except Exception as e:
        logging.error(f"Error al insertar o actualizar el artista en MySQL: {e}")
        return None


########################################################################################################################
# Inserción y actualización en MongoDB
########################################################################################################################

def upsert_artist_in_mongo(db_mongo, artist_upserted):
    """
    Inserta o actualiza un artista en MongoDB en la colección 'artists'.
    
    :param db_mongo: Conexión a la base de datos MongoDB.
    :param artist_upserted: Diccionario con los datos del artista.
    """
    try:
        # Filtro para buscar el artista por su identificador único
        search_filter = {'id_artist': artist_upserted.get('id_artist')}
        
        # Ejecutar la operación upsert (actualiza si existe, inserta si no)
        result = db_mongo['artists'].replace_one(
            search_filter,               # Filtro: busca el documento con el mismo id_artist
            artist_upserted,             # Reemplaza todo el documento
            upsert=True                  # Si no existe el documento, lo inserta
        )

        if result.matched_count > 0:
            logging.info(f"Artista con ID {artist_upserted.get('id_artist')} fue reemplazado en MongoDB.")
        else:
            logging.info(f"Artista con ID {artist_upserted.get('id_artist')} fue insertado en MongoDB.")
    
    except Exception as e:
        logging.error(f"Error al insertar o actualizar el artista en MongoDB: {e}")


########################################################################################################################
# Función principal para insertar o actualizar artistas
########################################################################################################################

def upsert_artist(db_mongo, db_pool, json_dict, ddex_map, album, update_id_message, insert_id_message):
    """
    Extrae los artistas del XML y los inserta o actualiza en MySQL y MongoDB.
    
    :param db_mongo: Conexión a MongoDB.
    :param db_pool: Pool de conexiones a la base de datos MySQL.
    :param json_dict: Diccionario JSON con los datos del XML.
    :param ddex_map: Mapeo de las rutas del DDEX definidas en el archivo ddex_43.yaml.
    :return: Lista de artistas insertados o actualizados.
    """
    logging.info("Iniciando el proceso de inserción o actualización de artistas...")
    
    # Obtener los artistas desde el XML
    artists_from_xml = get_artist_from_xml(json_dict, ddex_map)
    logging.info(f"Artistas extraídos desde el XML: {json_util.dumps(artists_from_xml, ensure_ascii=False, indent=4)}")
    
    # Iterar sobre los artistas y realizar el upsert en MySQL y MongoDB
    for artist in artists_from_xml:
        artist_upserted = upsert_artist_in_db(db_pool, artist, album, update_id_message, insert_id_message)  # Inserta o actualiza en MySQL
        if artist_upserted:
            upsert_artist_in_mongo(db_mongo, artist_upserted)  # Inserta o actualiza en MongoDB
    
    return artists_from_xml
