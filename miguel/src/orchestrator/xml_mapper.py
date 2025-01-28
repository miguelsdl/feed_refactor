from bson import json_util
import json
import logging
import re
from datetime import datetime, timedelta
import connections
from pymongo import UpdateOne, ReplaceOne


# Funciones Auxiliares

def duration_to_seconds(duration):
    """
    Convierte una cadena de duración en formato ISO 8601 a segundos.
    
    :param duration: Cadena de duración en formato ISO 8601 (e.g., 'PT1H30M45S').
    :return: Duración total en segundos.
    :raises ValueError: Si el formato de la duración es inválido.
    """
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)
    
    if not match:
        raise ValueError(f"Formato de duración no válido: {duration}")

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds


def seconds_to_hhmmss(seconds):
    """
    Convierte una cantidad de segundos a formato hh:mm:ss.
    
    :param seconds: Duración en segundos.
    :return: Cadena en formato hh:mm:ss.
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def get_value_from_path(json_dict, path):
    """
    Extrae un valor de un diccionario JSON usando una ruta de claves separadas por '/'.
    Puede manejar namespaces en las claves.

    :param json_dict: Diccionario JSON del cual extraer el valor.
    :param path: Ruta separada por '/' para acceder al valor.
    :return: El valor encontrado en la ruta especificada.
    :raises KeyError: Si alguna clave no se encuentra en el diccionario.
    """
    keys = path.split('/')
    for key in keys:
        if ':' in key:
            # Remover el namespace de la clave
            key_without_ns = key.split(':')[1]  # Obtener la clave sin el namespace
        else:
            key_without_ns = key

        # Intentar acceder a la clave con o sin namespace
        if key in json_dict:
            json_dict = json_dict[key]
        elif key_without_ns in json_dict:
            json_dict = json_dict[key_without_ns]
        else:
            raise KeyError(f"Key '{key}' not found in path '{path}'")
    
    return json_dict


def timedelta_to_string(td):
    """
    Convierte un objeto timedelta en una cadena de texto en formato 'HH:MM:SS'.
    
    :param td: Objeto timedelta a convertir.
    :return: Cadena en formato 'HH:MM:SS'.
    """
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def datetime_to_string(dt):
    """
    Convierte un objeto datetime en una cadena de texto en formato ISO 8601.
    
    :param dt: Objeto datetime a convertir.
    :return: Cadena en formato ISO 8601.
    """
    return dt.isoformat()


def serialize_with_custom_types(data):
    """
    Serializa un diccionario o lista, manejando tipos de datos especiales como timedelta y datetime.
    
    :param data: Diccionario o lista a serializar.
    :return: El diccionario o lista serializado, con los tipos timedelta y datetime convertidos a cadenas.
    """
    if isinstance(data, list):
        return [serialize_with_custom_types(item) for item in data]
    elif isinstance(data, dict):
        return {k: serialize_with_custom_types(v) for k, v in data.items()}
    elif isinstance(data, timedelta):
        return timedelta_to_string(data)
    elif isinstance(data, datetime):
        return datetime_to_string(data)
    else:
        return data
    


def convert_timedelta_to_seconds(td):
    """
    Convierte un objeto datetime.timedelta a segundos.
    
    :param td: Objeto timedelta.
    :return: El valor en segundos como un entero.
    """
    if isinstance(td, timedelta):
        return int(td.total_seconds())
    return td

def get_album_data(release_list):
    album_data = dict()

    try:
        album_data['artist'] = dict()

        artist = release_list['Release']['DisplayArtist']
        if isinstance(artist, dict):
            artist = [artist, ]

        for o in artist:
            album_data['artist'][o['ArtistPartyReference']] = o

        album_data['upc'] = release_list['Release']['ReleaseId']['ICPN']
        album_data['genre'] = release_list['Release']['Genre']
    except Exception as e:
        logging.error("Error al procesar los datos del album: " + str(e))
        print(e)

    return album_data

def get_party_list(party_list):
    party_name_list = dict()
    for o in party_list:
        if isinstance(o['PartyName'], dict):
            party_name_list[o['PartyReference']] = o['PartyName']['FullName']
        else:
            party_name_list[o['PartyReference']] = o['PartyName'][0]['FullName']

    return party_name_list

def get_album_id_from_db(conn, album_upc):
    sql = "SELECT id_album, upc_album FROM feed.albums WHERE upc_album = '{}';".format(album_upc)
    rows = connections.execute_query(conn, sql, {})
    album_data = {"album_id": rows[0][0], "album_upc": rows[0][1]}

    return album_data

def get_dict_by_language_code_in_list(vals, lang_code_order=('es', 'en'), key_name='@LanguageAndScriptCode'):
    if isinstance(vals, list):
        for v in vals:
            if v[key_name] == lang_code_order[0]:
                return v
            elif v[key_name] == lang_code_order[1]:
                return v
            else:
                return v
    else:
        return vals

def list_to_sql_in_str(data):
    return "('" + "', '".join(data) + "')"

def get_resource_list_data_by_resource_reference(resource_list):
    resource_data = dict()

    recording_ref_list = resource_list['SoundRecording']
    if not isinstance(recording_ref_list, list):
        recording_ref_list = list(resource_list['SoundRecording'])

    for sound_recording in recording_ref_list:
        resource_data[sound_recording['ResourceReference']] = sound_recording

    return resource_data

def get_dict_to_list_dict(dict_or_list):
    return [dict_or_list, ] if isinstance(dict_or_list, dict) else dict_or_list

def get_album_upc(release_data):
    return release_data['R0']['ReleaseId']['ICPN']

def get_album_dist(release_data):
    pass

def get_album_label(release_data):
    return release_data['R0']['ReleaseLabelReference']['#text']

def get_album_cmt(deal_data):
    return deal_data['R0']['DealTerms']['CommercialModelType']

def get_album_use_type(deal_data):
    return deal_data['R0']['DealTerms']['UseType']

def get_album_territory_code(deal_data):
    return deal_data['R0']['DealTerms']['TerritoryCode']

def get_album_start_date(deal_data):
    return deal_data['R0']['DealTerms']['ValidityPeriod']['StartDateTime']

def get_album_end_date(deal_data):
    try:
        return deal_data['R0']['DealTerms']['ValidityPeriod']['EndDateTime']
    except Exception as e:
        #logging.error("Error, no se encontro edn date: " + str(e))
        return None

def get_data_from_db(db_pool, name_fields, talbe_name, where_field, in_values, execute=True):
    sql_tpl = "SELECT {name_fields} FROM feed.{talbe_name} WHERE {where_field} IN ({in_values});"
    sql = sql_tpl.format(name_fields=name_fields, talbe_name=talbe_name, where_field=where_field, in_values=in_values)
    if execute:
        query_values = {}
        res = connections.execute_query(db_pool, sql, query_values)

        data_return = []
        field_list = name_fields.split(',')
        if res:
            for i in range(0, len(res)):
                data_dict = dict()
                for k in range(0, len(field_list)):
                    data_dict[field_list[k]] = res[i][k]
                data_return.append(data_dict)

        return data_return
    else:
        return sql

def get_release_list_sort_by_release_reference(release_list, key='Release'):
    release_data = dict()

    release_list = get_dict_to_list_dict(release_list[key])
    for rel in release_list:
        release_data[rel['ReleaseReference']] = rel

    return release_data

def get_deal_list_sort_by_release_reference(deal_list):
    deals_data = dict()
    # deal_list: {'ReleaseDeal': {'Deal': [{'DealTerms': {'CommercialModelType': 'SubscriptionModel',
    #                                          'TerritoryCode': ['AG', 'AI', 'AW', 'BB', 'BM', 'BO', 'BQ', 'BR', 'BZ',
    #                                                            'CL', 'CO', 'CR', 'CW', 'DM', 'DO', 'EC', 'ES', 'GD',
    #                                                            'GF', 'GP', 'GY', 'HT', 'JM', 'KN', 'KY', 'LC', 'MQ',
    #                                                            'MS', 'PA', 'PE', 'SR', 'SV', 'TC', 'TT', 'VC', 'VG',
    #                                                            'VU', 'ZM'],
    #                                          'UseType': ['ConditionalDownload', 'NonInteractiveStream',
    #                                                      'OnDemandStream'],
    #                                          'ValidityPeriod': {'StartDateTime': '2024-11-06T00:00:00'}}}, {
    #                               'DealTerms': {'CommercialModelType': 'AdvertisementSupportedModel',
    #                                             'TerritoryCode': ['AG', 'AI', 'AW', 'BB', 'BM', 'BO', 'BQ', 'BR', 'BZ',
    #                                                               'CL', 'CO', 'CR', 'CW', 'DM', 'DO', 'EC', 'ES', 'GD',
    #                                                               'GF', 'GP', 'GY', 'HT', 'JM', 'KN', 'KY', 'LC', 'MQ',
    #                                                               'MS', 'PA', 'PE', 'SR', 'SV', 'TC', 'TT', 'VC', 'VG',
    #                                                               'VU', 'ZM'],
    #                                             'UseType': ['NonInteractiveStream', 'OnDemandStream'],
    #                                             'ValidityPeriod': {'StartDateTime': '2024-11-06T00:00:00'}}}],
    #                  'DealReleaseReference': 'R1'}}
    release_deal_list = get_dict_to_list_dict(deal_list['ReleaseDeal'])
    # selease_deal_list: [{'Deal': [{'DealTerms': {'CommercialModelType': 'SubscriptionModel',
    #                           'TerritoryCode': ['AG', 'AI', 'AW', 'BB', 'BM', 'BO', 'BQ', 'BR', 'BZ', 'CL', 'CO', 'CR',
    #                                             'CW', 'DM', 'DO', 'EC', 'ES', 'GD', 'GF', 'GP', 'GY', 'HT', 'JM', 'KN',
    #                                             'KY', 'LC', 'MQ', 'MS', 'PA', 'PE', 'SR', 'SV', 'TC', 'TT', 'VC', 'VG',
    #                                             'VU', 'ZM'],
    #                           'UseType': ['ConditionalDownload', 'NonInteractiveStream', 'OnDemandStream'],
    #                           'ValidityPeriod': {'StartDateTime': '2024-11-06T00:00:00'}}}, {
    #                'DealTerms': {'CommercialModelType': 'AdvertisementSupportedModel',
    #                              'TerritoryCode': ['AG', 'AI', 'AW', 'BB', 'BM', 'BO', 'BQ', 'BR', 'BZ', 'CL', 'CO',
    #                                                'CR', 'CW', 'DM', 'DO', 'EC', 'ES', 'GD', 'GF', 'GP', 'GY', 'HT',
    #                                                'JM', 'KN', 'KY', 'LC', 'MQ', 'MS', 'PA', 'PE', 'SR', 'SV', 'TC',
    #                                                'TT', 'VC', 'VG', 'VU', 'ZM'],
    #                              'UseType': ['NonInteractiveStream', 'OnDemandStream'],
    #                              'ValidityPeriod': {'StartDateTime': '2024-11-06T00:00:00'}}}],
    #   'DealReleaseReference': 'R1'}]
    for d in release_deal_list:
        data = d['DealReleaseReference']
        if not isinstance(d['DealReleaseReference'], list):
            data = [d['DealReleaseReference'], ]
        for ref in data:
            deals_data[ref] = d['Deal'][0]
        deals_data['R0'] = d['Deal'][1]

    return deals_data

def get_value_by_key_from_dict_inverted(data, key):
    return {v:k for k, v in data.items()}.get(key)

def get_party_liat_for_ref(party_list):
    names = dict()
    value = None
    for party in party_list:
        key = party['PartyReference']
        if isinstance(party['PartyName'], list):
            value = party['PartyName'][0]['FullName']

        else:
            value = party['PartyName']['FullName']
        names[key] = value
    return names


def safe_parse(s):
    final = []
    try:
        step1 = s.replace('true', 'True').replace('null', " 'IS NULL' ").replace('JSON_OBJECT', '')
        step2 = list(eval(step1)) # s.replace("(", '').replace(')', '')

        for st in step2:
            final.append(st)
            # if isinstance(st, str):
            #     if st == '1':
            #         final.append(True)
            #     else:
            #         final.append(st)
            # else:
            #     final.append(st)

    except Exception as e:
        logging.error("Error en safe_parse(), string origial: {} Exception: {}".format(s, e))
        final = []

    return final


def merge_fields_name_with_values_tuple(fields, where_conditions):
    where = []
    for k in where_conditions:
        if "USSM19805796" in k:
            print(2)
        tup = safe_parse(k)
        condition_by_tup = list()
        for i in range(0, len(fields)):
            field = fields[i]
            value = tup[i]
            if isinstance(value, str):
                val = "{}='{}'".format(field, value.replace('"', '').replace("'", ""))
            else:
                val = "{}={}".format(field, value)

            if val not in where:
                condition_by_tup.append(val)
        condition = list(set(condition_by_tup))

        if len(condition) > 0:
            where.append(" AND ".join(condition))
    return where


def get_select_of_last_updated_insert_fields(fields, table_name, where_conditions):
    sql_where = merge_fields_name_with_values_tuple(
        fields,
        where_conditions
    )

    sql = " SELECT {} FROM feed.{} WHERE {};" \
        .format("*", table_name, " OR ".join(sql_where))
    is_or = sql[len(sql)-6:]
    if is_or ==" OR ;":
        sql = sql.replace(is_or, "")



    # .format(",".join(("name_label", "active_label")), "labels", " and ".join(sql_where))

    return sql

def update_in_mongo_db2(db_mongo, rows, table_name, structure):

    if rows:
        upsert_list = []
        for row in rows:
            legacy_rows_to_list = dict()
            r = list(row)
            legacy_rows_to_list["_id"] = r[0]
            # legacy_rows_to_list["values"] = json.dumps(r, default=json_util.default)
            search_filter = {'_id': r[0]}
            # "_id": r[0],

            for i in range(0, len(structure)):
                legacy_rows_to_list[structure[i]] = r[i]

            # Ejecutar la operación upsert (actualiza si existe, inserta si no)
            result = db_mongo[table_name].replace_one(
                search_filter,  # Filtro: busca el documento con el mismo id_artist
                legacy_rows_to_list,  # Reemplaza todo el documento
                upsert=True  # Si no existe el documento, lo inserta
            )
        #     upsert_list.append(ReplaceOne(
        #         search_filter,
        #         {"values": r}
        #         ))
        # result = db_mongo['{}'.format(table_name)].bulk_write(upsert_list)
        logging.error("upsert en mongo")

    return True

# label_mongodb_structure = {
#     "id_label": None,
#     "name_label": '',
#     "active_label": False,
#     "audi_edited_label": '',
#     "audi_created_label": '',
#     "update_id_message": '',
#     "insert_id_message": ''
# }
