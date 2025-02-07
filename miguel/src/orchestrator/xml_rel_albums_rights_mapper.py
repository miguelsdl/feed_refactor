from sqlalchemy import text
import json
import logging
import connections
import xml_mapper


def get_tack_data_from_db(conn, track_data_list):
    db_track_data = dict()
    sql_in = "('" + "','".join(track_data_list.keys()) + "')"
    sql = "SELECT id_track, isrc_track FROM feed.tracks WHERE isrc_track IN {};".format(sql_in)
    rows = connections.execute_query(conn, sql, {})
    for r in rows:
        db_track_data[r[1]] = r[0]

    return db_track_data

def get_data_from_db(db_pool, name_fields, talbe_name, where_field, in_values):
    sql_tpl = "SELECT {name_fields} FROM feed.{talbe_name} WHERE {where_field} IN ({in_values});"
    sql = sql_tpl.format(name_fields=name_fields, talbe_name=talbe_name, where_field=where_field, in_values=in_values)
    query_values = {}
    res = connections.execute_query(db_pool, sql, query_values)

    data_return = []
    field_list = name_fields.split(',')
    for i in range(0, len(res)):
        data_dict = dict()
        for k in range(0, len(field_list)):
            data_dict[field_list[k]] = res[i][k]
        data_return.append(data_dict)

    return data_return

def get_album_label(release_data, ref_id):
    return release_data[ref_id]['ReleaseLabelReference']['#text']

def get_album_cmt(deal_data, ref_id):
    return deal_data[ref_id]['DealTerms']['CommercialModelType']

def get_album_use_type(deal_data, ref_id):
    return deal_data[ref_id]['DealTerms']['UseType']

def get_album_territory_code(deal_data, ref_id):
    return deal_data[ref_id]['DealTerms']['TerritoryCode']

def get_album_start_date(deal_data, ref_id):
    return deal_data[ref_id]['DealTerms']['ValidityPeriod']['StartDateTime']

def get_album_end_date(deal_data, ref_id):
    try:
        return deal_data[ref_id]['DealTerms']['ValidityPeriod']['EndDateTime']
    except Exception as e:
        logging.error("Error, no se encontro end date: " + str(e))
        return None


def upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, id_dist):
    rows = list()


    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    release_data = xml_mapper.get_release_list_sort_by_release_reference(release_list)
    ref_alb = xml_mapper.get_release_list_release_reference_album(release_data)

    deal_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealList'])
    deal_data = xml_mapper.get_deal_list_sort_by_release_reference(deal_list)

    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_data = xml_mapper.get_party_list(party_list)

    album_upc = xml_mapper.get_album_upc(release_data, ref_alb)
    album_label = get_album_label(release_data, ref_alb)
    album_cmt = get_album_cmt(deal_data, ref_alb)
    album_use_type = get_album_use_type(deal_data, ref_alb)
    territory_code = get_album_territory_code(deal_data, ref_alb)
    start_date = get_album_start_date(deal_data, ref_alb)
    end_date = get_album_end_date(deal_data, ref_alb)

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

    sql_in = []
    query_values = []
    coutries_list = xml_mapper.get_countries_from_db(db_pool)
    for lb in album_label_data:
        id_label = lb['id_label']
        for cmt in album_cmt_data:
            id_cmt = cmt['id_cmt']
            for u in album_use_type_data:
                id_use_type = u['id_use_type']
                # countries = json.dumps(territory_code)
                countries = json.dumps(xml_mapper.get_countries_id_by_iso_code_list(coutries_list, territory_code))
                sql_tmp = [
                    album_data[0]['id_album'], 1, id_label, id_cmt, id_use_type,
                    countries, "'" + start_date + "'", end_date if end_date else None, insert_id_message
                ]
                sql_in.append("(" + ",".join([ "{}".format(x) for x in sql_tmp]) + ")")

                query_values.append({
                    "id_album": album_data[0]['id_album'],
                    "id_dist": id_dist,
                    "id_label": id_label,
                    "id_cmt": id_cmt,
                    "id_use_type": id_use_type,
                    "cnty_ids_albright": countries,
                    "start_date_albright": start_date,
                    "end_date_albright": end_date,
                    'insert_id_message': insert_id_message,
                    "update_id_message": update_id_message
                })

    upsert_query = text("""
    INSERT INTO feed.albums_rights (
        id_album, id_dist, id_label, id_cmt, id_use_type, cnty_ids_albright, start_date_albright,
        end_date_albright, insert_id_message, audi_edited_albright, update_id_message
    )
    VALUES (
        :id_album, :id_dist, :id_label, :id_cmt, :id_use_type, :cnty_ids_albright, :start_date_albright,
        :end_date_albright, :insert_id_message, CURRENT_TIMESTAMP, :update_id_message
    )
    ON DUPLICATE KEY UPDATE
        id_album = id_album, 
        id_dist = id_dist, 
        id_label = id_label, 
        id_cmt = id_cmt, 
        id_use_type = id_use_type, 
        cnty_ids_albright = cnty_ids_albright, 
        start_date_albright = start_date_albright, 
        end_date_albright = end_date_albright, 
        audi_edited_albright = CURRENT_TIMESTAMP,
        update_id_message = {} 
    """.format(update_id_message))

    connections.execute_query(db_pool, upsert_query, query_values, list_map=True)
    logging.info("Se ejecutó la consulta upsert en mysql")
    return True

def upsert_rel_album_right(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, id_dist):
    upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, id_dist)
