from sqlalchemy import text
import json
import logging
import connections
import xml_mapper


def get_track_isrc(release_list):
    track_isrc_list = dict()
    for track in release_list:
        track_isrc_list[track] = release_list[track]['ReleaseId']['ProprietaryId']['#text']

    return track_isrc_list

def get_track_label(track_list_data, party_data):
    track_label = dict()

    for track in track_list_data:
        track_label[track] = party_data[track_list_data[track]['ReleaseLabelReference']['#text']]

    return track_label

def get_track_cmt(cmt_data):
    cmt = dict()
    for deal in cmt_data:
        if deal not in cmt:
            cmt[deal] = list()
        if isinstance(cmt_data[deal], list):
            for each in cmt_data[deal]:
                cmt[deal].append(each['DealTerms']['CommercialModelType'])
        else:
            cmt[deal].append(cmt_data[deal]['DealTerms']['CommercialModelType'])

    return cmt

def get_track_use_type(use_type_data):
    use_type_list = dict()
    for u in use_type_data:
        # ['ConditionalDownload', 'NonInteractiveStream', 'OnDemandStream']
        if not u in use_type_list:
            use_type_list[u] = list()
        if isinstance(use_type_data[u], list):
            for each in use_type_data[u]:
                uts = each['DealTerms']['UseType']
                if not isinstance(uts, list):
                    use_type_list[u].update([uts, ])
                else:
                    use_type_list[u].append(uts)
        else:
            uts = use_type_data[u]['DealTerms']['UseType']
            if not isinstance(uts, list):
                use_type_list[u].update([uts, ])
            else:
                use_type_list[u].append(uts)

    return use_type_list

# def get_track_territory_code(deal_data):
#     codes = dict()
#     for deal in deal_data:
#         codes[deal] = deal_data[deal]['DealTerms']['TerritoryCode']
#
#     return codes

def get_track_start_date(deal_data):
    track_start_date = dict()
    for k in deal_data:
        if isinstance(deal_data[k], list):
            for each in deal_data[k]:
                track_start_date[k] = each['DealTerms']['ValidityPeriod']['StartDateTime']
        else:
            track_start_date[k] = deal_data[k]['DealTerms']['ValidityPeriod']['StartDateTime']

    return track_start_date

def get_track_end_date(deal_data):
    track_end_date = dict()
    for k in deal_data:
        if isinstance(deal_data[k], list):
            for each in deal_data[k]:
                period = each['DealTerms']['ValidityPeriod']
                if "EndDateTime" in period:
                    track_end_date[k] = each['DealTerms']['ValidityPeriod']['EndDateTime']
                else:
                    track_end_date[k] = None
        else:
            period = deal_data[k]['DealTerms']['ValidityPeriod']
            if "EndDateTime" in period:
                track_end_date[k] = deal_data[k]['DealTerms']['ValidityPeriod']['EndDateTime']
            else:
                track_end_date[k] = None

    return track_end_date

def get_deal_list_sort_by_release_reference(deal_list):
    deals_data = dict()
    release_deal_list = xml_mapper.get_dict_to_list_dict(deal_list['ReleaseDeal'])

    for d in release_deal_list:
        data = d['DealReleaseReference']
        if not isinstance(d['DealReleaseReference'], list):
            data = [d['DealReleaseReference'], ]

        for ref in data:
            deals = xml_mapper.get_dict_to_list_dict(d['Deal'])
            for deal in deals:
                if ref not in deals_data:
                    deals_data[ref] = list()
                deals_data[ref].append(deal)
        if len(d['Deal']) == 2:
            deals_data['R0'] = d['Deal'][1]

    return deals_data

def get_data_from_xml(json_dict, ddex_map):

    release_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ReleaseList'])
    album_data = xml_mapper.get_release_list_sort_by_release_reference(release_list)
    track_list_data = xml_mapper.get_release_list_sort_by_release_reference(release_list, key="TrackRelease")
    deal_list = xml_mapper.get_value_from_path(json_dict, ddex_map['DealList'])
    deal_data = get_deal_list_sort_by_release_reference(deal_list)
    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_data = xml_mapper.get_party_list(party_list)
    resource_list = xml_mapper.get_value_from_path(json_dict, ddex_map['ResourceList'])
    cmt_and_use_type_association = dict()
    # for deal in deal_list['ReleaseDeal']['Deal']:
    #     cmt_and_use_type_association[deal['DealTerms']['CommercialModelType'].strip()] = deal['DealTerms']['UseType']

    rd_list = xml_mapper.get_dict_to_list_dict(deal_list['ReleaseDeal'])
    for deal in rd_list:
        if not isinstance(deal['Deal'], list):
            deal_list_ =  [deal['Deal'], ]
        else:
            deal_list_ = deal['Deal']

        for d in deal_list_:
            cmt_and_use_type_association[d['DealTerms']['CommercialModelType'].strip()] = d['DealTerms']['UseType']

    return {
        "release_list": release_list, "album_data": album_data, "track_list_data": track_list_data,
        "deal_list": deal_list, "deal_data": deal_data, "party_list": party_list, "party_data": party_data,
        "resource_list": resource_list, 'cmt_and_use_type_association': cmt_and_use_type_association,
    }

def get_val_join_xml_and_db_data(xml_data, db_data, reference):
    id = []
    for row in db_data:
        vals = list(row.values())
        try:
            if vals[1] in xml_data[reference]:
                id.append(vals[0])
        except Exception as e:
            logging.error("error en get_val_join_xml_and_db_data(): " + str(e))

    return id

def get_id_album_track(db_pool, id_album, id_track):
    sql = "select id_album_track from feed.albums_tracks where id_album = {} and id_track = {};"\
           .format(id_album, id_track)
    rows = connections.execute_query(db_pool, sql, {})
    return rows[0][0] if rows else None

def get_resource_list(data):
    data_return = {}
    sr_list = data['SoundRecording'] if isinstance(data['SoundRecording'], list) else [data['SoundRecording'], ]
    for sr in sr_list:
        try:
            data_return[sr['SoundRecordingEdition']['ResourceId']['ISRC']] = {
                "pline_year": sr['SoundRecordingEdition']['PLine']['Year'],
                "pline_text": sr['SoundRecordingEdition']['PLine']['PLineText'],
            }
        except Exception as e:
            raise e


    return data_return

def get_territory_code_by_deal_term(dl):
    if isinstance(dl['DealTerms']['TerritoryCode'], list):
        ret = dl['DealTerms']['TerritoryCode']
    else:
        ret = [dl['DealTerms']['TerritoryCode'], ]
    return ret

def get_cmt_by_deal_term(dl):
    try:
        if isinstance(dl['DealTerms']['CommercialModelType'], list):
            ret = dl['DealTerms']['CommercialModelType']
        else:
            ret = [dl['DealTerms']['CommercialModelType'], ]
    except Exception as e:
        raise e
    return ret

def get_use_type_by_deal_term(dl):
    if isinstance(dl['DealTerms']['UseType'], list):
        ret = dl['DealTerms']['UseType']
    else:
        ret = [dl['DealTerms']['UseType'], ]
    return ret

def get_start_date_by_deal_term(dl):
    return dl['DealTerms']['ValidityPeriod']['StartDateTime']

def get_end_date_by_deal_term(dl):
    try:
        return dl['DealTerms']['ValidityPeriod']['EndDateTime']
    except Exception as e:
        return None

def get_track_territory_code(deal_data):
    dict_ret = dict()


    # {'DealTerms': {'CommercialModelType': 'SubscriptionModel5', 'TerritoryCode': ['AG', 'AI',
    # 'AW', 'BB', 'BM', 'BO', 'BQ', 'BR', 'BZ', 'CL', 'CO', 'CR', 'CW', 'DM', 'DO', 'EC', 'ES',
    # 'GD', 'GF', 'GP', 'GY', 'HT', 'JM', 'KN', 'KY', 'LC', 'MQ', 'MS', 'PA', 'PE', 'SR', 'SV', 'TC', 'TT', 'VC',
    # 'VG', 'VU', 'ZM'], 'UseType': ['ConditionalDownload2', 'NonInteractiveStream3', 'OnDemandStream4'],
    # 'ValidityPeriod': {'EndDateTime': '2029-11-06T00:00:00', 'StartDateTime': '1990-04-09T00:00:00'}}}
    try:
        for ref, deal_terms in deal_data.items():
            deal_terms_list = deal_terms if isinstance(deal_terms, list) else [deal_terms, ]
            for dl in deal_terms_list:
                print(1)
                for cmt in get_cmt_by_deal_term(dl):
                    for use in get_use_type_by_deal_term(dl):
                        key = "{}:{}".format(cmt, use)
                        value = {
                            "start_date": get_start_date_by_deal_term(dl),
                            "end_date": get_end_date_by_deal_term(dl),
                            "codes": get_territory_code_by_deal_term(dl),
                        }

                        if key not in dict_ret:
                            dict_ret[key] = []
                        dict_ret[key].append(value)
    except Exception as e:
        raise e
        print(e)
    return dict_ret


# def get_track_territory_code(deal_data):
#     return deal_data['R0']['DealTerms']['TerritoryCode']


def upsert_rel_album_track_right_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, id_dist):
    data_from_xml = get_data_from_xml(json_dict, ddex_map)
    ref_alb = xml_mapper.get_release_list_release_reference_album(data_from_xml['album_data'])
    album_upc = xml_mapper.get_album_upc( data_from_xml['album_data'], ref_alb)
    track_isrc = get_track_isrc( data_from_xml['track_list_data'])
    track_label = get_track_label( data_from_xml['track_list_data'], data_from_xml['party_data'])
    track_cmt = get_track_cmt(data_from_xml['deal_data'])
    track_use_type = get_track_use_type(data_from_xml['deal_data'])
    territory_code = get_track_territory_code(data_from_xml['deal_data'])
    start_date = get_track_start_date(data_from_xml['deal_data'])
    end_date = get_track_end_date(data_from_xml['deal_data'])
    resource_list = get_resource_list(data_from_xml['resource_list'])


    album_data = xml_mapper.get_data_from_db(
        db_pool, 'id_album,upc_album',"albums", "upc_album", album_upc
    )
    sql_in =  "'" + "','".join(list(track_isrc.values())) + "'"
    track_ids_data = xml_mapper.get_data_from_db(
        db_pool, 'id_track,isrc_track',"tracks",
        "isrc_track", sql_in,
    )
    # merge track_isrc y track_ids_data
    track_isrc_invert = {v:k for k, v in track_isrc.items()}
    track_id_by_ref = dict()
    for tck in track_ids_data:
        isrc = tck['isrc_track']
        track_id_by_ref[tck['id_track']] = track_isrc_invert[isrc]

    sql_in = "'" + "','".join(set(track_label.values())) + "'"
    track_label_data = xml_mapper.get_data_from_db(
        db_pool, 'id_label,name_label',"labels", "name_label", sql_in
    )

    cmt_set = set()
    for cmt in track_cmt.values():
        if isinstance(cmt, list):
            for c in cmt:
                cmt_set.add(c)
        else:
            cmt_set.add(cmt)

    sql_in = "'" + "','".join(cmt_set) + "'"
    track_cmt_data = xml_mapper.get_data_from_db(
        db_pool, 'id_cmt,name_cmt',"comercial_model_types", "name_cmt", sql_in
    )

    tr_vals = list(track_use_type.values())
    uniq_vals = set()
    for tr in tr_vals:
        for t in tr:
            for k in t:
                uniq_vals.add(k)

    sql_in_ = "'" + "','".join(uniq_vals) + "'"

    track_use_type_data = xml_mapper.get_data_from_db(
        db_pool, 'id_use_type,name_use_type',"use_types", "name_use_type", sql_in_
    )

    sql_in = []
    coutries_list = xml_mapper.get_countries_from_db(db_pool)
    for track in track_ids_data:
        id_track = track["id_track"]
        track_reference = xml_mapper.get_value_by_key_from_dict_inverted(track_isrc, track["isrc_track"])
        id_label = get_val_join_xml_and_db_data(track_label, track_label_data, track_reference)
        id_cmt = get_val_join_xml_and_db_data(track_cmt, track_cmt_data, track_reference)
        id_use_type = get_val_join_xml_and_db_data(track_use_type, track_use_type_data, track_reference)
        id_album_track = get_id_album_track(db_pool, album_data[0]['id_album'], id_track)
        ref = track_id_by_ref[id_track]
        # cnty_ids_albtraright = json.dumps(territory_code)

        if isinstance(start_date, dict):
            if start_date[ref]:
                start_date = start_date[ref]
            else:
                start_date = 'null'

        if isinstance(end_date, dict):
            if end_date[ref]:
                end_date = end_date[ref]
            else:
                end_date = None
        ref____ = ref
        track_cmt_data______ = track_cmt_data
        # [{'id_cmt': 1, 'name_cmt': 'SubscriptionModel5'}, {'id_cmt': 2, 'name_cmt': 'SubscriptionModel'},
        #  {'id_cmt': 3, 'name_cmt': 'AdvertisementSupportedModel'}]
        cmt_is_assoc = dict()
        for tr in track_cmt_data:
            cmt_is_assoc[tr["name_cmt"]] = tr["id_cmt"]

        # for cmt_ in track_cmt_data:
        for cmt_ in track_cmt[ref]:
            for utype in track_use_type_data:
                # search_list = data_from_xml['cmt_and_use_type_association'][cmt_['name_cmt']]
                search_list = track_use_type[ref][0]
                ut_name = utype['name_use_type']
                if ut_name in search_list:
                    if "{}:{}".format(cmt_, ut_name) in territory_code:
                        values = territory_code["{}:{}".format(cmt_, ut_name)].pop()
                        countries_codes = values.get("codes")
                        cnty_ids_albtraright = xml_mapper.get_countries_id_by_iso_code_list(coutries_list, countries_codes)
                        start_date = values.get("start_date")
                        end_date = values.get("end_date")
                        sql_tmp = {
                            "id_album_track": id_album_track,
                            "id_dist": id_dist,
                            "id_label": id_label[0],
                            "id_cmt": cmt_is_assoc[cmt_],
                            "id_use_type": utype['id_use_type'],
                            "cnty_ids_albtraright": "{}".format(cnty_ids_albtraright),
                            "start_date_albtraright": "{}".format(start_date),
                            "end_date_albtraright": "{}".format(end_date) if end_date is not None else None,
                            "insert_id_message": insert_id_message,
                            "pline_text_albtraright": resource_list[track["isrc_track"]]['pline_text'],
                            "pline_year_albtraright": resource_list[track["isrc_track"]]['pline_year'],
                            "update_id_message": update_id_message
                        }
                        sql_in.append(sql_tmp)

    query_values = sql_in

    upsert_query = text("""
            INSERT INTO feed.albums_tracks_rights (
                id_album_track, id_dist, id_label, id_cmt, id_use_type, cnty_ids_albtraright, start_date_albtraright, 
                end_date_albtraright, insert_id_message, audi_edited_albtraright, pline_text_albtraright, 
                pline_year_albtraright, update_id_message
            )
            VALUES (
                :id_album_track, :id_dist, :id_label, :id_cmt, :id_use_type, :cnty_ids_albtraright, 
                :start_date_albtraright, :end_date_albtraright, :insert_id_message, CURRENT_TIMESTAMP, 
                :pline_text_albtraright, :pline_year_albtraright, :update_id_message
            )
            ON DUPLICATE KEY UPDATE
                id_album_track = id_album_track,
                id_dist = id_dist,
                id_label = id_label,
                id_cmt = id_cmt,
                id_use_type = id_use_type,
                cnty_ids_albtraright = cnty_ids_albtraright,
                start_date_albtraright = start_date_albtraright,
                end_date_albtraright = end_date_albtraright,
                audi_edited_albtraright = CURRENT_TIMESTAMP,
                pline_text_albtraright = pline_text_albtraright,
                pline_year_albtraright = pline_year_albtraright,
                update_id_message={}
            """.format(update_id_message)
        )

    connections.execute_query(db_pool, upsert_query, query_values, list_map=True)
    logging.info("Se ejecutó la consulta upsert en mysql")

    return True

def upsert_rel_album_track_right(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, id_dist):
    upsert_rel_album_track_right_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, id_dist)
