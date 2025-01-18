import logging
import json
from sqlalchemy import text
from bson import json_util
import xml_mapper
import connections
from datetime import timedelta

import xml_artist_mapper 


########################################################################################################################
# Funciones para asociar SoundRecording con TrackRelease
#############################################################################################

def build_resource_map(json_dict, ddex_map):
    """
    Construye un diccionario donde la clave es ResourceReference y el valor es el objeto SoundRecording.
    
    :param json_dict: Diccionario JSON con los datos del XML.
    :param ddex_map: Mapeo de las rutas del DDEX definidas en el archivo ddex_43.yaml.
    :return: Diccionario con los SoundRecordings mapeados por ResourceReference.
    """
    try:
        resource_list = xml_mapper.get_value_from_path(json_dict, ddex_map['SoundRecording'])
        resource_map = {}

        # Verificar si resource_list es un solo diccionario o una lista
        if isinstance(resource_list, dict):
            resource_list = [resource_list]  # Convertir en lista si es un solo elemento
        
        # Crear el mapa de resource_reference -> sound_recording
        for sound_recording in resource_list:
            resource_reference = sound_recording.get('ResourceReference')
            if resource_reference:
                resource_map[resource_reference] = sound_recording

        return resource_map
    except KeyError as e:
        logging.error(f"Error al construir el mapa de SoundRecordings: {e}")
        return {}


def get_sound_recording_for_track(track_release, resource_map):
    """
    Obtiene el SoundRecording correspondiente para un TrackRelease dado usando el ResourceReference.
    
    :param track_release: Diccionario que representa un track dentro de TrackRelease.
    :param resource_map: Diccionario que mapea ResourceReference a SoundRecording.
    :return: Diccionario con la información de SoundRecording correspondiente, o None si no se encuentra.
    """
    try:
        release_resource_reference = track_release.get('ReleaseResourceReference')
        return resource_map.get(release_resource_reference)
    except KeyError as e:
        logging.error(f"Error al obtener el SoundRecording para el TrackRelease: {e}")
        return None
    

def get_release_resource_sequence(release, track_release_reference):
    """
    Obtiene el número de secuencia correspondiente a un ReleaseResourceReference en el Release.
    
    :param release: Diccionario que representa el Release principal.
    :param track_release_reference: El ReleaseResourceReference del TrackRelease.
    :return: SequenceNumber correspondiente al ReleaseResourceReference o None si no se encuentra.
    """
    try:
        # Obtener la lista de ResourceGroup en el Release
        resource_groups = release.get('ResourceGroup', [])
        if isinstance(resource_groups, dict):
            resource_groups = [resource_groups]  # Convertir a lista si solo hay un ResourceGroup

        # Iterar sobre los grupos de recursos
        for resource_group in resource_groups:
            # Obtener los ResourceGroupContentItem dentro del ResourceGroup
            content_items = resource_group.get('ResourceGroupContentItem', [])
            if isinstance(content_items, dict):
                content_items = [content_items]  # Convertir a lista si solo hay un ResourceGroupContentItem

            # Iterar sobre cada item en el grupo de contenido
            for content_item in content_items:
                # Verificar si el ReleaseResourceReference coincide
                release_resource_reference = content_item.get('ReleaseResourceReference')
                if release_resource_reference == track_release_reference:
                    return content_item.get('SequenceNumber')  # Retornar el SequenceNumber si coincide

        # Si no se encuentra, retornar None
        return None

    except KeyError as e:
        logging.error(f"Error al obtener el número de secuencia para el recurso: {e}")
        return None    


########################################################################################################################
# Funciones de obtención de valores en el archivo DDEX
########################################################################################################################

def get_track_name(track_release, sound_recording):
    """
    Obtiene el nombre del track desde el diccionario de TrackRelease utilizando el campo DisplayTitle.
    Si no se encuentra, intenta obtenerlo desde SoundRecording utilizando el campo DisplayTitleText o DisplayTitle.

    :param track_release: Diccionario que representa un track dentro de TrackRelease.
    :param sound_recording: Diccionario que representa un SoundRecording.
    :return: Nombre del track o 'Unknown' si no se encuentra.
    """
    try:
        # Primero intenta obtener el título desde track_release
        display_title = track_release.get('DisplayTitle', [])
        if isinstance(display_title, dict):
            display_title = [display_title]

        for title in display_title:
            if title.get('@IsDefault') == 'true':
                return title.get('TitleText', 'Unknown')
        
        # Si no hay título por defecto, intenta devolver el primer título disponible
        if display_title:
            return display_title[0].get('TitleText', 'Unknown')
        
        # Si no se encontró el título en track_release, intenta obtenerlo desde sound_recording
        display_title_text = sound_recording.get('DisplayTitleText', {})
        if isinstance(display_title_text, dict) and '#text' in display_title_text:
            return display_title_text['#text']
        
        # Como alternativa, intenta obtenerlo desde DisplayTitle en sound_recording
        display_title = sound_recording.get('DisplayTitle', {})
        if isinstance(display_title, dict):
            return display_title.get('TitleText', 'Unknown')
        
        return 'Unknown'
    except KeyError as e:
        logging.error(f"Error al obtener el nombre del track: {e}")
        return 'Unknown'


def get_track_subtitle(track_release, sound_recording):
    """
    Obtiene el subtítulo del track desde el diccionario de TrackRelease utilizando el campo DisplayTitle.
    Si no se encuentra, intenta obtenerlo desde SoundRecording utilizando el campo DisplayTitle.

    :param track_release: Diccionario que representa un track dentro de TrackRelease.
    :param sound_recording: Diccionario que representa un SoundRecording.
    :return: Subtítulo del track o None si no se encuentra.
    """
    def format_subtitle(subtitle):
        # Verifica si el subtítulo es un diccionario con @SubTitleType y #text
        if isinstance(subtitle, dict):
            subtitle_type = subtitle.get('@SubTitleType', '').strip()
            subtitle_text = subtitle.get('#text', '').strip()
            # Retorna en el formato "SubTitleType: text" si ambos están presentes
            if subtitle_type and subtitle_text:
                return f"{subtitle_type}: {subtitle_text}"
            return subtitle_text or None
        return subtitle  # Si no es un diccionario, retorna el valor tal cual
    
    try:
        # Intentar obtener el subtítulo desde track_release
        display_title = track_release.get('DisplayTitle', [])
        if isinstance(display_title, dict):
            display_title = [display_title]

        for title in display_title:
            if title.get('@IsDefault') == 'true':
                return format_subtitle(title.get('SubTitle', None))
        
        # Si no hay subtítulo por defecto, intenta devolver el primer subtítulo disponible
        if display_title:
            return format_subtitle(display_title[0].get('SubTitle', None))
        
        # Si no se encontró el subtítulo en track_release, intenta obtenerlo desde sound_recording
        display_title = sound_recording.get('DisplayTitle', {})
        if isinstance(display_title, dict):
            return format_subtitle(display_title.get('SubTitle', None))
        
        return None
    except KeyError as e:
        logging.error(f"Error al obtener el subtítulo del track: {e}")
        return None
    

def getParentalWarning(sound_recording):
    """
    Verifica si el tipo de advertencia de contenido explícito está presente en los datos del sound_recording.
    Devuelve 1 si el contenido es explícito, 0 en caso contrario.
    """
    parental_warning = sound_recording.get('ParentalWarningType')
    
    if parental_warning is None:
        return 0
    
    # Verifica si el valor es directamente 'ExplicitContentEdited'
    if parental_warning == 'ExplicitContentEdited':
        return 1
    
    # Si parental_warning es un diccionario, intenta obtener el valor de '#text'
    if isinstance(parental_warning, dict) and parental_warning.get('#text') == 'ExplicitContentEdited':
        return 1

    return 0


def get_track_isrc(track_release):
    """
    Obtiene el ISRC del track desde el diccionario de TrackRelease.
    
    :param track_release: Diccionario que representa un track dentro de TrackRelease.
    :return: ISRC del track.
    """
    try:
        isrc = track_release.get('ReleaseId', {}).get('ISRC', None)

        if isrc is None:

            # Para casos en que el ISRC esta directo dentro del ReleasId y no hay Propietary Id
            release_id = track_release.get('ReleaseId', {})

            # Verifica que 'ProprietaryId' esté presente y que el '@Namespace' sea 'ISRC'
            if release_id.get('ProprietaryId', {}).get('@Namespace') == 'ISRC':
                isrc = release_id['ProprietaryId'].get('#text')
            else:
                isrc = None  # En caso de que no sea un ISRC

        return isrc
    except KeyError as e:
        logging.error(f"Error al obtener el ISRC del track: {e}")
        return None


def get_track_artists_from_sound_recording(sound_recording, party_list):
    """
    Obtiene los artistas asociados al track desde el diccionario de SoundRecording usando la referencia de PartyList.
    
    :param sound_recording: Diccionario que representa un SoundRecording dentro del XML.
    :param party_list: Lista de diccionarios que contiene los datos de los artistas en 'PartyList'.
    :return: Lista de diccionarios con la información de los artistas.
    """
    try:
        display_artist_list = sound_recording.get('DisplayArtist', [])
        if isinstance(display_artist_list, dict):
            display_artist_list = [display_artist_list]

        artists = []
        for display_artist in display_artist_list:
            artist_party_reference = display_artist.get('ArtistPartyReference')

            # Buscar el nombre del artista en PartyList usando ArtistPartyReference
            artist_name = None
            for party in party_list:
                if party.get('PartyReference') == artist_party_reference:
                    artist_name = xml_artist_mapper.get_artist_name(party)
                    break

            if artist_name:
                artist_data = {
                    'artist_name': artist_name,
                    'artist_role': display_artist.get('DisplayArtistRole', 'Unknown')
                }
                artists.append(artist_data)

        return artists
    except KeyError as e:
        logging.error(f"Error al obtener los artistas del sound recording: {e}")
        return []
    

def get_artist_id_from_db(db_pool, artist_name):
    """
    Recupera el ID del artista desde la base de datos MySQL usando su nombre.
    
    :param db_pool: Pool de conexiones a la base de datos.
    :param artist_name: Nombre del artista a buscar.
    :return: ID del artista o None si no se encuentra.
    """
    try:
        select_query = text("SELECT id_artist FROM feed.artists WHERE name_artist = :artist_name")
        query_values = {'artist_name': artist_name}
        artist_rows = connections.execute_query(db_pool, select_query, query_values)

        if artist_rows:
            return artist_rows[0].id_artist
        else:
            return None
    except Exception as e:
        logging.error(f"Error al obtener el ID del artista desde la base de datos: {e}")
        return None


def get_tracks_from_xml(json_dict, ddex_map):
    """
    Extrae la información de los tracks desde el XML mapeado.
    
    :param json_dict: Diccionario JSON con los datos del XML.
    :param ddex_map: Mapeo de las rutas del DDEX definidas en el archivo ddex_43.yaml.
    :return: Lista de diccionarios con información de los tracks.
    """
    try:
        # Construir el mapa ResourceReference -> SoundRecording
        resource_map = build_resource_map(json_dict, ddex_map)

        party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])

        track_release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['TrackRelease'])
        if isinstance(track_release_list, dict):
            track_release_list = [track_release_list]

        tracks = []
        sequence = 1
        for track_release in track_release_list:

            # Obtener el SoundRecording correspondiente para el track actual
            sound_recording = get_sound_recording_for_track(track_release, resource_map)

            track = {
                'isrc_track': get_track_isrc(track_release),
                'name_track': get_track_name(track_release, sound_recording),
                'version_track': get_track_subtitle(track_release, sound_recording),
                'length_track': xml_mapper.seconds_to_hhmmss(xml_mapper.duration_to_seconds(sound_recording.get('Duration'))),
                'explicit_track': getParentalWarning(sound_recording),
                'active_track': 1,  # Predeterminado a activo
                'specific_data_track': {"available_128": True, "available_320": True, "available_preview": True},
                'insert_id_message': 0,
                'update_id_message': 0,
                'volume_album_track': 1,
                'number_album_track': sequence,
                'artists': get_track_artists_from_sound_recording(sound_recording, party_list)  # Obtener los artistas desde SoundRecording usando PartyList
            }
            tracks.append(track)
            sequence=sequence+1
        return tracks
    except KeyError as e:
        logging.error(f"Error al obtener la lista de tracks: {e}")
        return []


########################################################################################################################
# Inserción y actualización en batch para MySQL
########################################################################################################################

def get_tracks_from_db(db_pool, isrc_tracks):
    """
    Recupera los tracks asociados a un conjunto de ISRCs desde la base de datos MySQL usando una cláusula IN.
    
    :param db_pool: Pool de conexiones a la base de datos.
    :param isrc_tracks: Lista de ISRCs de los tracks a buscar.
    :return: Diccionario con los datos de los tracks, indexado por 'isrc_track'.
    """
    try:
        # Si no hay ISRCs en la lista, no hacemos la consulta
        if not isrc_tracks:
            return []

        # Formatear la consulta para usar una cláusula IN
        select_query = text("""
            SELECT * 
            FROM feed.tracks 
            WHERE isrc_track IN :isrc_tracks
        """)

        # Ejecutar la consulta con la lista de ISRCs
        query_values = {'isrc_tracks': tuple(isrc_tracks)}  # Se convierte en tupla para usar con SQLAlchemy IN

        track_rows = connections.execute_query(db_pool, select_query, query_values)

        if track_rows:
            # Retornar los resultados como un diccionario indexado por 'isrc_track'
            return {track['isrc_track']: dict(track._mapping.items()) for track in track_rows}
        else:
            return {}

    except Exception as e:
        logging.error(f"Error al obtener los tracks desde la base de datos: {e}")
        return {}
    

def get_tracks_list_from_db(db_pool, isrc_tracks):
    """
    Recupera un conjunto de tracks desde la base de datos MySQL usando una cláusula IN con múltiples ISRCs.
    
    :param db_pool: Pool de conexiones a la base de datos.
    :param isrc_tracks: Iterable con múltiples ISRCs de los tracks a buscar.
    :return: Lista de diccionarios con los datos de los tracks.
    """
    try:
        # Asegurarse de que isrc_tracks es un iterable y no está vacío
        if not isrc_tracks:
            return []

        # Usar la cláusula IN para buscar múltiples ISRCs
        select_query = text("SELECT * FROM feed.tracks WHERE isrc_track IN :isrc_tracks")

        select_query = text("""
                            SELECT 
                                t.*, 
                                JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'id_artist', a.id_artist,
                                        'name_artist', a.name_artist
                                    )
                                ) AS artists
                            FROM 
                                feed.tracks t 
                            JOIN 
                                feed.tracks_artists ta ON ta.id_track = t.id_track 
                            JOIN 
                                feed.artists a ON a.id_artist = ta.id_artist 
                            WHERE 
                                t.isrc_track IN :isrc_tracks
                            GROUP BY 
                                t.id_track;	
        """)

        
        # Convertir la lista de ISRCs en una tupla (requerido para cláusula IN en SQLAlchemy)
        query_values = {'isrc_tracks': tuple(isrc_tracks)}

        # Ejecutar la consulta
        tracks_rows = connections.execute_query(db_pool, select_query, query_values)

        # Si se encontraron filas, devolverlas como una lista de diccionarios
        if tracks_rows:
            tracks_dict = [dict(row._mapping.items()) for row in tracks_rows]
            return tracks_dict
        else:
            return []

    except Exception as e:
        logging.error(f"Error al obtener los tracks desde la base de datos: {e}")
        return []
    

def upsert_tracks_in_db(db_pool, tracks_from_xml, id_album):
    """
    Inserta o actualiza tracks en la base de datos MySQL en batch.
    
    :param db_pool: Pool de conexiones a la base de datos MySQL.
    :param tracks_from_xml: Lista de tracks obtenidos desde el XML.
    :param id_album: Identificador del álbum.
    :return: Tracks insertados o actualizados.
    """
    try:
        # Obtener los ISRCs de los tracks del XML
        isrc_tracks = [track['isrc_track'] for track in tracks_from_xml if track['isrc_track']]
        
        # Obtener los tracks actuales desde la base de datos usando los ISRCs
        existing_tracks = get_tracks_from_db(db_pool, isrc_tracks)

        # Dividir en dos listas: para insertar y para actualizar
        tracks_to_insert = []
        tracks_to_update = []

        for track in tracks_from_xml:
            isrc = track['isrc_track']
            if isrc in existing_tracks:
                # Track ya existe, preparar para actualización
                tracks_to_update.append(track)
            else:
                # Track nuevo, preparar para inserción
                tracks_to_insert.append(track)

        # Inserción batch
        if tracks_to_insert:
            insert_query = text("""
                INSERT INTO feed.tracks (
                    isrc_track, name_track, version_track, length_track, explicit_track,
                    active_track, specific_data_track, insert_id_message, update_id_message
                ) 
                VALUES (
                    :isrc_track, :name_track, :version_track, :length_track, :explicit_track,
                    :active_track, :specific_data_track, :insert_id_message, :update_id_message
                )
            """)
            connections.execute_query_batch(db_pool, insert_query, tracks_to_insert)

        # Actualización batch
        if tracks_to_update:
            update_query = text("""
                UPDATE feed.tracks 
                SET 
                    name_track = :name_track,
                    version_track = :version_track,
                    active_track = :active_track,
                    update_id_message = :update_id_message,
                    audi_edited_track = CURRENT_TIMESTAMP
                WHERE isrc_track = :isrc_track
            """)
            connections.execute_query_batch(db_pool, update_query, tracks_to_update)


        albums_tracks_to_insert = []
        tracks_artists_to_insert = []
        inserted_tracks = get_tracks_from_db(db_pool, isrc_tracks)
        for track in tracks_from_xml:
            isrc = track['isrc_track']
            if isrc in inserted_tracks:
                # Generar los datos para albums_tracks
                albums_tracks_to_insert.append({
                    'id_album': id_album,
                    'id_track': inserted_tracks.get(isrc).get('id_track'),  # Actualizará después de la inserción de tracks
                    'volume_album_track': track.get('volume_album_track'),  # Puedes cambiarlo según sea necesario
                    'number_album_track': track.get('number_album_track'),  # El número de pista es el índice en la lista
                    'insert_id_message': 0,
                    'update_id_message': 0
                })

                # Generar los datos para tracks_artists
                for artist in track.get('artists', []):
                    artist_id = get_artist_id_from_db(db_pool, artist['artist_name'])
                    if artist_id:
                        tracks_artists_to_insert.append({
                            'id_track': inserted_tracks.get(isrc).get('id_track'),
                            'id_artist': artist_id,  # Obtener el ID del artista
                            'artist_role_track_artist': artist.get('artist_role'),
                            'insert_id_message': 0,
                            'update_id_message': 0,
                            'active_track_artist': 1
                        })

                        if inserted_tracks.get(isrc).get('id_track') is None:
                            logging.error("El campo 'id_track' es obligatorio y no puede ser None.")
                            return False  # O alguna otra acción adecuada

                        
        # Inserción batch para albums_tracks
        insert_album_tracks_query = text("""
            INSERT INTO feed.albums_tracks (
                id_album, id_track, volume_album_track, number_album_track, insert_id_message, update_id_message
            )
            VALUES (
                :id_album, :id_track, :volume_album_track, :number_album_track, :insert_id_message, :update_id_message
            )
            ON DUPLICATE KEY UPDATE
                update_id_message = VALUES(update_id_message),
                audi_edited_album_track = CURRENT_TIMESTAMP
        """)
        connections.execute_query_batch(db_pool, insert_album_tracks_query, albums_tracks_to_insert)

        # Inserción batch para tracks_artists
        insert_tracks_artists_query = text("""
            INSERT INTO feed.tracks_artists (
                id_track, id_artist, artist_role_track_artist, insert_id_message, update_id_message, active_track_artist
            )
            VALUES (
                :id_track, :id_artist, :artist_role_track_artist, :insert_id_message, :update_id_message, :active_track_artist
            )
            ON DUPLICATE KEY UPDATE
                update_id_message = VALUES(update_id_message),
                audi_edited_track_artist = CURRENT_TIMESTAMP
        """)
        connections.execute_query_batch(db_pool, insert_tracks_artists_query, tracks_artists_to_insert)


        tracks_from_db = get_tracks_list_from_db(db_pool, isrc_tracks)

        return tracks_from_db #tracks_from_xml

    except Exception as e:
        logging.error(f"Error al insertar o actualizar tracks en MySQL en batch: {e}")
        return None



########################################################################################################################
# Inserción y actualización en batch para MongoDB
########################################################################################################################

def upsert_tracks_in_mongo_old(db_mongo, tracks_upserted):
    """
    Inserta o actualiza tracks en MongoDB en batch, convirtiendo tipos no soportados como timedelta.
    
    :param db_mongo: Conexión a MongoDB.
    :param tracks_upserted: Lista de tracks que han sido insertados o actualizados.
    """
    try:
        for track in tracks_upserted:
            # Convertir timedelta en segundos
            if 'length_track' in track:
                track['length_track'] = xml_mapper.convert_timedelta_to_seconds(track['length_track'])
            
            # Convertir otros campos si es necesario (manejar datetime.datetime si es necesario)
            track = {k: (str(v) if isinstance(v, timedelta) else v) for k, v in track.items()}

            # Agregar lista de artistas al JSON interno
            track['artists'] = [{'id_artist': artist['id_artist'], 'name_artist': artist['artist_name']} for artist in track.get('artists', [])]

            # Inserción o actualización en MongoDB
            search_filter = {'isrc_track': track.get('isrc_track')}
            db_mongo['tracks'].replace_one(search_filter, track, upsert=True)

        logging.info(f"Tracks insertados/actualizados en MongoDB.")
    except Exception as e:
        logging.error(f"Error al insertar o actualizar tracks en MongoDB en batch: {e}")


def upsert_tracks_in_mongo(db_mongo, tracks_upserted):
    """
    Inserta o actualiza tracks en MongoDB en batch, convirtiendo tipos no soportados como timedelta.
    
    :param db_mongo: Conexión a MongoDB.
    :param tracks_upserted: Lista de tracks que han sido insertados o actualizados.
    """
    try:
        for track in tracks_upserted:
            # Convertir timedelta en segundos
            if 'length_track' in track:
                track['length_track'] = xml_mapper.convert_timedelta_to_seconds(track['length_track'])
            
            # Convertir otros campos si es necesario (manejar datetime.datetime si es necesario)
            track = {k: (str(v) if isinstance(v, timedelta) else v) for k, v in track.items()}

            # Convertir 'artists' de JSON string a JSON array si es necesario
            if isinstance(track.get('artists'), str):
                try:
                    track['artists'] = json.loads(track['artists'])
                except json.JSONDecodeError as e:
                    logging.error(f"Error al decodificar el JSON de artistas: {e}")
                    track['artists'] = []

            # Agregar lista de artistas al JSON interno
            track['artists'] = [{'id_artist': artist.get('id_artist'), 'name_artist': artist.get('name_artist')} for artist in track.get('artists', []) if isinstance(artist, dict)]

            # Inserción o actualización en MongoDB
            search_filter = {'isrc_track': track.get('isrc_track')}
            db_mongo['tracks'].replace_one(search_filter, track, upsert=True)

        logging.info(f"Tracks insertados/actualizados en MongoDB.")
    except Exception as e:
        logging.error(f"Error al insertar o actualizar tracks en MongoDB en batch: {e}")

########################################################################################################################
# Función principal para procesar tracks en batch
########################################################################################################################

def upsert_tracks(db_mongo, db_pool, json_dict, ddex_map, album):
    """
    Extrae los tracks del XML y los inserta o actualiza en MySQL y MongoDB en batch.
    
    :param db_mongo: Conexión a MongoDB.
    :param db_pool: Pool de conexiones a la base de datos MySQL.
    :param json_dict: Diccionario JSON con los datos del XML.
    :param ddex_map: Mapeo de las rutas del DDEX definidas en el archivo ddex_43.yaml.
    :param album: Información del álbum para asociar los tracks.
    :return: Lista de tracks insertados o actualizados.
    """
    logging.info("Iniciando el procesamiento de tracks en batch...")

    # Obtener los tracks desde el XML
    tracks_from_xml = get_tracks_from_xml(json_dict, ddex_map)
    logging.info(f"Tracks extraídos desde el XML: {json_util.dumps(tracks_from_xml, ensure_ascii=False, indent=4)}")

    # Insertar o actualizar en MySQL en batch
    tracks_upserted = upsert_tracks_in_db(db_pool, tracks_from_xml, album['id_album'])

    if tracks_upserted:
        # Insertar o actualizar en MongoDB en batch
        upsert_tracks_in_mongo(db_mongo, tracks_upserted)

    return tracks_upserted