import logging
from sqlalchemy import text
import connections
import xml_mapper

def get_party_list_map(json_dict, ddex_map):
    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_list_ref = xml_mapper.get_party_liat_for_ref(party_list)
    party_list_ref_inverted = {v:k for k,v in party_list_ref.items()}

    return {"party_list_ref": party_list_ref, "party_list_ref_inverted": party_list_ref_inverted}

def get_artist_list_from_sound_recording(ref, display_artist, party_list_maps):
    artist = []
    artist_name = list()

    if isinstance(display_artist, dict):
        display_artist = [display_artist, ]

    party_reference = party_list_maps.get('party_list_ref', dict())


    for da in display_artist:
        artist.append({
            "artist_name": party_reference.get(da['ArtistPartyReference']),
            "role": da['DisplayArtistRole'],
            "party_ref": da['ArtistPartyReference'],
        })
        artist_name.append(party_reference.get(da['ArtistPartyReference']), )

    return {"artist": artist, "artist_name": {ref: artist_name}}

def get_track_data(db_pool, isrc_map):
    sql = []
    for key, val in isrc_map.items():
        sql.append(
            xml_mapper.get_data_from_db(
                db_pool, "'{}',id_track,isrc_track","tracks", "isrc_track",
                "'{}'", execute=False
            ).format(key, val)
        )
    sql = " UNION ".join(sql).replace(";", " \n ")
    # Acá ya tengo una lista de los tracks de esta forma [('A1', 1, 'USSM19805796'),...]
    rows = connections.execute_query(db_pool, sql, {})

    return rows

def get_artist_data(db_pool, artist_data):
    sql = []
    for key, val in artist_data.items():
        sql.append(
            xml_mapper.get_data_from_db(
                db_pool, "'{}',id_artist,name_artist", "artists", "name_artist", {}, execute=False
            ).format(key, "'" + "','".join(val) + "'")
        )
    sql = " UNION ".join(sql).replace(";", " \n ")
    rows = connections.execute_query(db_pool, sql, {})

    return rows

def upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):

    sound_recording = xml_mapper.get_value_from_path(json_dict, ddex_map['SoundRecording'])
    if not isinstance(sound_recording, list):
        sound_recording = [sound_recording, ]

    # esto devuelvel un diccionario de dos elementos
    party_list_maps = get_party_list_map(json_dict, ddex_map)

    data_track_artis_by_reference = dict()
    isrc_map = {}
    artist_names_once = set()
    all_artist = dict()
    artist_rol_by_ref = {}
    for sr in sound_recording:
        ref = sr['ResourceReference']
        artist_rol_by_ref[ref] = {}
        isrc = sr['SoundRecordingEdition']['ResourceId']['ISRC']
        isrc_map.setdefault(ref, isrc)
        artist_data = get_artist_list_from_sound_recording(ref, sr['DisplayArtist'], party_list_maps)
        artist_list = artist_data.get('artist')
        data_track_artis_by_reference[ref] = {"track_isrc": isrc,"artist_list": artist_list,}
        for a in artist_list:
            artist_rol_by_ref[ref].setdefault(a.get('artist_name'), a.get('role'))

        all_artist.update(artist_data.get('artist_name'))

    track_data = get_track_data(db_pool, isrc_map)
    tracks_id_by_ref = {k[0]: k[1] for k in track_data}
    artist = get_artist_data(db_pool, all_artist)
    artist_id_by_ref = {k[0]: k[1] for k in artist }
    artist_id_by_ref = {}
    for a in artist:
        artist_ref, artist_id, artist_name = a[0], a[1], a[2]
        if artist_ref not in artist_id_by_ref:
            artist_id_by_ref[artist_ref] = list()
        artist_id_by_ref[artist_ref].append({"artist_id": artist_id, "artist_name": artist_name})

    rows = list()
    for key, val in tracks_id_by_ref.items():
        artists_list = artist_id_by_ref[key]
        roles = artist_rol_by_ref[key]
        for a in artists_list:
            rows.append({
                'id_track': val,
                'id_artist': a['artist_id'],
                "artist_role_track_artist": roles[a['artist_name']],
                "insert_id_message": insert_id_message,
                "update_id_message": update_id_message
            })

    upsert_query = text("""
    INSERT INTO feed.tracks_artists (
        id_track, id_artist, artist_role_track_artist,insert_id_message, update_id_message, audi_edited_track_artist
    )
    VALUES (
        :id_track, :id_artist, :artist_role_track_artist, :insert_id_message, :update_id_message, CURRENT_TIMESTAMP
    )
    ON DUPLICATE KEY UPDATE
        id_track = id_track,
        id_artist = id_artist,
        artist_role_track_artist = artist_role_track_artist,
        insert_id_message = insert_id_message,
        audi_edited_track_artist = CURRENT_TIMESTAMP,
        update_id_message={}
    """.format(update_id_message))
    connections.execute_query(db_pool, upsert_query, rows, list_map=True)
    logging.info("Se ejecutó la consulta upsert en mysql")

def upsert_rel_track_artist(db_mongo, db_pool, json_dict, ddex_map):
    upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map)
