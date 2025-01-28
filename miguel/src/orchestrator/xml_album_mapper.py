import logging
import json 
from sqlalchemy import text
import connections
import xml_mapper

########################################################################################################################
# Funciones de obtencion de valores en archivo DDEX
########################################################################################################################

def get_DisplayTitleText(release):
    try:
        display_title_text = release['DisplayTitleText']

        # Si DisplayTitleText es una lista, buscar preferentemente el título en español, luego en inglés
        if isinstance(display_title_text, list):
            name = None
            for title in display_title_text:
                language_code = title.get('@LanguageAndScriptCode', '').lower()
                if language_code.startswith('es'):  # Preferentemente español
                    name = title.get('#text')
                    break  # Encontrado en español, salir del bucle
                elif language_code.startswith('en'):  # Guardar el inglés como opción secundaria
                    name = title.get('#text')
            
            if name is None:  # Si no se encontró ni en español ni en inglés
                name = display_title_text[0].get('#text', 'Desconocido')
        
        elif isinstance(display_title_text, dict):
            # Si es un diccionario, acceder directamente
            name = display_title_text.get('#text', 'Desconocido')
        
        else:
            name = 'Desconocido'

        logging.info(f"Name: {name}")
        return name
    
    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None    


def get_Duration(release, sound_recording):
    try:
        # Intentar obtener la duración desde release['Duration']
        Duration = release.get('Duration')

        if not Duration:  # Si no encuentra 'Duration' en release, pasa a calcularlo desde sound_recording
            logging.info("No se encontró 'Duration' en release. Calculando desde SoundRecording...")

            # Verificar la estructura de SoundRecording (Suma los Segundos de todos los Tracks)
            total_duration_seconds = 0
            if isinstance(sound_recording, list):
                for sr in sound_recording:
                    if isinstance(sr, dict) and 'Duration' in sr:
                        total_duration_seconds += xml_mapper.duration_to_seconds(sr['Duration'])
                    else:
                        logging.info(f"Elemento inesperado en SoundRecording: {sr}")
            elif isinstance(sound_recording, dict):
                if 'Duration' in sound_recording:
                    total_duration_seconds = xml_mapper.duration_to_seconds(sound_recording['Duration'])
                else:
                    logging.info(f"Elemento inesperado en SoundRecording: {sound_recording}")
            else:
                raise ValueError("SoundRecording no es ni una lista ni un diccionario. Verifica la estructura.")

            # Convertir la duración total a HH:MM:SS
            total_duration_hhmmss = xml_mapper.seconds_to_hhmmss(total_duration_seconds)
            logging.info(f"Duración total: {total_duration_hhmmss}")
            return total_duration_hhmmss
        
        else:
            # Si se encuentra 'Duration' en release, simplemente retorna esa duración
            duration_seconds = xml_mapper.duration_to_seconds(Duration)
            total_duration_hhmmss = xml_mapper.seconds_to_hhmmss(duration_seconds)
            logging.info(f"Duración desde release: {total_duration_hhmmss}")
            return total_duration_hhmmss
    
    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None


def get_OriginalReleaseDate(release):
    try:
        # Intentar obtener el OriginalReleaseDate directamente
        original_release_date = release.get('OriginalReleaseDate')

        if isinstance(original_release_date, dict):
            # Si OriginalReleaseDate es un diccionario, buscar el valor bajo '#text'
            return original_release_date.get('#text', '2900-01-01')
        elif isinstance(original_release_date, str):
            # Si OriginalReleaseDate ya es una cadena de texto, retornarla directamente
            return original_release_date
        else:
            logging.info(f"Formato inesperado para OriginalReleaseDate: {original_release_date}")
            return '2900-01-01'

    except KeyError as e:
        logging.info(f"Clave no encontrada: {e}")
        return None   


def get_album_from_xml(json_dict, ddex_map):

    # Obtiene el ICPN
    ICPN = xml_mapper.get_value_from_path(json_dict, ddex_map['ICPN'])
    logging.info(f"ICPN: {ICPN}")

    # Obtiene el Album
    Release = xml_mapper.get_value_from_path(json_dict, ddex_map['Release'])
    #logging.info(f"Release: {Release}")

    # Obtiene los Tracks
    TrackRelease = xml_mapper.get_value_from_path(json_dict, ddex_map['TrackRelease'])

    # Obtiene los Audios
    SoundRecording = xml_mapper.get_value_from_path(json_dict, ddex_map['SoundRecording'])

    # Comprobar si TrackRelease es una lista, si no lo es, convertirlo en una lista para manejar múltiples tracks
    if isinstance(TrackRelease, dict):
        TrackRelease = [TrackRelease]  # Si solo hay un Track, lo convertimos en una lista de un elemento

    # Carga el Album
    album_xml = {
        "id_album": None,
        "upc_album": Release['ReleaseId']['ICPN'],
        "name_album": get_DisplayTitleText(Release), # Release['DisplayTitleText']['#text'],
        "subtitle_album": None,
        "release_type_album": Release['ReleaseType'],
        "length_album": get_Duration(Release, SoundRecording), # total_duration_hhmmss, 
        "tracks_qty_album": len(TrackRelease),
        "release_date_album": get_OriginalReleaseDate(Release), # Release['OriginalReleaseDate']['#text'],
        "active_album": 1,
        "specific_data_album": {"tracks_qty_available_128": len(TrackRelease)}
    }

    return album_xml, ICPN


def get_album_from_db(db_pool, upc):
    # Consulta SQL
    select_query = text(f"""SELECT * from feed.albums where upc_album = :upc_album""")
    query_values = {
        'upc_album': upc,  # SQLAlchemy convierte None a NULL
    }
    
    # Ejecutar la consulta
    album_rows = connections.execute_query(db_pool, select_query, query_values)

    # Convertir cada fila a un diccionario
    if album_rows:
        album_dicts = []
        for row in album_rows:
            album_dict = dict(row._mapping.items())
            
            # Verificar si 'specific_data_album' es una cadena JSON y deserializarla
            if "specific_data_album" in album_dict and isinstance(album_dict["specific_data_album"], str):
                try:
                    album_dict["specific_data_album"] = json.loads(album_dict["specific_data_album"])
                except json.JSONDecodeError:
                    logging.info(f"Error al deserializar 'specific_data_album' para el álbum: {album_dict['name_album']}")
            
            album_dicts.append(album_dict)
    else:
        album_dicts = []

    # Convertir timedelta y datetime a un formato serializable antes de convertir a JSON
    album_dicts_serialized = xml_mapper.serialize_with_custom_types(album_dicts)

    # Convertir la lista de diccionarios a JSON
    album_json = json.dumps(album_dicts_serialized, indent=4)

    # Convertir Album_from_db a lista de diccionarios si es JSON
    if isinstance(album_json, str):
        try:
            Album_from_db = json.loads(album_json)
        except json.JSONDecodeError:
            logging.info("Error al convertir el álbum desde la base de datos a diccionario.")
            Album_from_db = []

    if len(Album_from_db) > 0:
        return Album_from_db[0]
    else: None


# Función para manejar None y escaparlo si es una cadena
def escape_or_null(value):
    if value is None:
        return 'NULL'  # Si el valor es None, insertamos NULL sin comillas
    # Reemplazar comillas simples por comillas dobles y envolver entre comillas simples
    return f"""{str(value).replace("'", "''")}"""


def upsert_album_in_db(db_pool, album_from_xml, update_id_message, insert_id_message):
    """
    Inserta o actualiza un álbum en la base de datos MySQL usando ON DUPLICATE KEY UPDATE.
    """

    # Construir la consulta SQL con parámetros bind
    upsert_query = text("""
    INSERT INTO feed.albums (
        id_album, upc_album, name_album, subtitle_album, release_type_album, length_album, 
        tracks_qty_album, release_date_album, active_album, specific_data_album, insert_id_message, update_id_message
    ) 
    VALUES (
        :id_album, :upc_album, :name_album, :subtitle_album, :release_type_album, :length_album, 
        :tracks_qty_album, :release_date_album, :active_album, :specific_data_album, :insert_id_message, :update_id_message
    )
    ON DUPLICATE KEY UPDATE
        name_album = VALUES(name_album),
        subtitle_album = VALUES(subtitle_album),
        release_type_album = VALUES(release_type_album),
        length_album = VALUES(length_album),
        tracks_qty_album = VALUES(tracks_qty_album),
        release_date_album = VALUES(release_date_album),
        active_album = VALUES(active_album),
        specific_data_album = VALUES(specific_data_album),
        update_id_message = VALUES(update_id_message),
        audi_edited_album = CURRENT_TIMESTAMP
    """)

    # Preparar los valores para la consulta
    query_values = {
        'id_album': album_from_xml.get('id_album', None),  # SQLAlchemy convierte None a NULL
        'upc_album': album_from_xml.get('upc_album', None),
        'name_album': album_from_xml.get('name_album', None),
        'subtitle_album': album_from_xml.get('subtitle_album', None),
        'release_type_album': album_from_xml.get('release_type_album', None),
        'length_album': album_from_xml.get('length_album', None),
        'tracks_qty_album': album_from_xml.get('tracks_qty_album', None),
        'release_date_album': album_from_xml.get('release_date_album', None),
        'active_album': album_from_xml.get('active_album', 0),
        'specific_data_album': json.dumps(album_from_xml.get('specific_data_album', {})),  # Convertir a JSON string
        'insert_id_message': insert_id_message,
        'update_id_message': update_id_message,
    }


    # Ejecutar la consulta con la función connections.execute_query
    connections.execute_query(db_pool, upsert_query, query_values)

    album_upserted = get_album_from_db(db_pool, album_from_xml.get('upc_album'))

    return album_upserted


def upsert_album_in_mongo(db_mongo, album_upserted, ICPN):
    """
    Inserta o actualiza un álbum en la colección 'albums' en MongoDB.
    Si ya existe un documento con el mismo ICPN en la ruta anidada, se actualiza; de lo contrario, se inserta.
    """
    # Ruta del ICPN dentro de la estructura del documento
    search_filter = {'upc_album': ICPN}
    
    # Ejecutar la operación upsert (actualiza si existe, inserta si no)
    result = db_mongo['albums'].replace_one(
        search_filter,               # Filtro: busca el documento con el mismo ICPN
        album_upserted,              # Reemplaza todo el documento
        upsert=True                  # Si no existe el documento, lo inserta
    )

    if result.matched_count > 0:
        logging.info(f"Álbum con ICPN {ICPN} fue reemplazado en MongoDB.")
    else:
        logging.info(f"Álbum con ICPN {ICPN} fue insertado en MongoDB.")        



def upsert_album(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):

    album_from_xml, ICPN = get_album_from_xml(json_dict, ddex_map)

    album_from_db = get_album_from_db(db_pool, ICPN)

    # Evaluar si el álbum ya existe en la base de datos
    if album_from_db:
        # Album_from_db es una lista de álbumes, obtener el primero
        album_db_dict = album_from_db[0] if isinstance(album_from_db, list) and album_from_db else album_from_db

        # Si existe, asignar el 'id_album' desde la base de datos al álbum obtenido del XML
        if album_db_dict and album_db_dict.get('upc_album') == album_from_xml.get('upc_album'):
            album_from_xml['id_album'] = album_db_dict.get('id_album')
 
    album_upserted = upsert_album_in_db(db_pool, album_from_xml, update_id_message, insert_id_message)

    if album_upserted:
        upsert_album_in_mongo(db_mongo, album_upserted, ICPN)

    return album_upserted


