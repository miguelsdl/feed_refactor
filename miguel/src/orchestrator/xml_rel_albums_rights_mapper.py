import json
import logging
import connections
import xml_mapper


def get_resource_list_data_by_resource_reference(resource_list):
    resource_data = dict()

    recording_ref_list = resource_list['SoundRecording']
    if not isinstance(recording_ref_list, list):
        recording_ref_list = list(recording_ref_list)

    for sound_recording in recording_ref_list:
        resource_data[sound_recording['ResourceReference']] = sound_recording

    return resource_data

def get_release_list_sort_by_release_reference(release_list):
    release_data = dict()

    release_list = xml_mapper.get_dict_to_list_dict(release_list['Release'])
    for rel in release_list:
        release_data[rel['ReleaseReference']] = rel

    return release_data

def get_deal_list_sort_by_release_reference(deal_list):
    deals_data = dict()

    release_deal_list = xml_mapper.get_dict_to_list_dict(deal_list['ReleaseDeal'])
    for d in release_deal_list:
        for ref in d['DealReleaseReference']:
            deals_data[ref] = d['Deal'][0]
        deals_data['R0'] = d['Deal'][1]

    return deals_data

def get_tack_data_from_db(conn, track_data_list):
    db_track_data = dict()
    sql_in = "('" + "','".join(track_data_list.keys()) + "')"
    sql = "SELECT id_track, isrc_track FROM feed.tracks WHERE isrc_track IN {};".format(sql_in)
    rows = connections.execute_query(conn, sql, {})
    for r in rows:
        db_track_data[r[1]] = r[0]

    return db_track_data

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
        logging.error("Error, no se encontro edn date: " + str(e))
        return None

def get_data_from_db(db_pool, name_fields, talbe_name, where_field, in_values):
    sql_tpl = "SELECT {name_fields} FROM feed.{talbe_name} WHERE {where_field} IN ({in_values});"
    sql = sql_tpl.format(name_fields=name_fields, talbe_name=talbe_name, where_field=where_field, in_values=in_values)
    query_values = {}
    res = connections.execute_query(db_pool, sql, query_values)
    if res is None:
        print(2)

    data_return = []
    field_list = name_fields.split(',')
    for i in range(0, len(res)):
        data_dict = dict()
        for k in range(0, len(field_list)):
            data_dict[field_list[k]] = res[i][k]
        data_return.append(data_dict)

    return data_return


def upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map):
    rows = list()
    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    release_data = get_release_list_sort_by_release_reference(release_list)

    deal_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealList'])
    deal_data = get_deal_list_sort_by_release_reference(deal_list)

    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_data = xml_mapper.get_party_list(party_list)

    album_upc = get_album_upc(release_data)
    album_label = get_album_label(release_data)
    album_cmt = get_album_cmt(deal_data)
    album_use_type = get_album_use_type(deal_data)
    territory_code = get_album_territory_code(deal_data)
    start_date = get_album_start_date(deal_data)
    end_date = get_album_end_date(deal_data)

    album_data = get_data_from_db(
        db_pool, 'id_album, upc_album',"albums", "upc_album", album_upc
    )

    sql_in = "'" + party_data[album_label] + "'"
    album_label_data = get_data_from_db(
        db_pool, 'id_label, name_label',"labels", "name_label", sql_in
    )

    sql_in = "'" + album_cmt + "'"
    album_cmt_data = get_data_from_db(
        db_pool, 'id_cmt, name_cmt',"comercial_model_types", "name_cmt", sql_in
    )

    sql_in = "'" + "','".join(album_use_type) + "'"
    album_use_type_data = get_data_from_db(
        db_pool, 'id_use_type, name_use_type',"use_types", "name_use_type", sql_in
    )

    # [{' upc_album': '886443370340', 'id_album': 3}]
    # [{' name_label': 'Epic/Legacy', 'id_label': 84}]
    # [{' name_cmt': 'AdvertisementSupportedModel', 'id_cmt': 2}]
    #[{' name_use_type': 'NonInteractiveStream', 'id_use_type': 2}, {' name_use_type': 'OnDemandStream', 'id_use_type': 3}]

    sql_in = []
    for lb in album_label_data:
        id_label = lb['id_label']
        for cmt in album_cmt_data:
            id_cmt = cmt['id_cmt']
            for u in album_use_type_data:
                id_use_type = u['id_use_type']
                countries = json.dumps(territory_code)
                sql_tmp = [
                    album_data[0]['id_album'], 1, id_label, id_cmt, id_use_type,
                    "'" + countries + "'", "'" + start_date + "'", "null"
                ]
                sql_in.append("(" + ",".join([ "{}".format(x) for x in sql_tmp]) + ")")

    print(0)

    # sql = "insert into albums_tracks (id_album, id_track) values {} ON DUPLICATE KEY UPDATE " \
    #        "audi_edited_album_track = CURRENT_TIMESTAMP;".format(",".join(sql_values))
    #
    sql = " insert into albums_rights " \
           " (id_album, id_dist, id_label, id_cmt, id_use_type, cnty_ids_albright, start_date_albright, " \
           " end_date_albright) " \
           " values {} " \
           " ON DUPLICATE KEY UPDATE " \
           "audi_created_albright = CURRENT_TIMESTAMP;".format(','.join(sql_in))



    rows = connections.execute_query(db_pool, sql, {})

    return rows

def upsert_rel_album_right(db_mongo, db_pool, json_dict, ddex_map):
    upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map)


