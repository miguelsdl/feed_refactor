import logging
from sqlalchemy import text
import connections
import xml_mapper



def upsert_use_track_contributor_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    sound_recording_map = dict()
    sound_recording = xml_mapper.get_value_from_path(json_dict, ddex_map['SoundRecording'])
    if not isinstance(sound_recording, list):
        sound_recording = [sound_recording, ]

    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_list_ref = xml_mapper.get_party_liat_for_ref(party_list)
    party_list_ref_inverted = {v:k for k,v in party_list_ref.items()}

    for sr in sound_recording:
            sound_recording_map[sr['ResourceReference']] = {
                "contributors": sr['Contributor'] if 'Contributor' in sr else None,
                "track_isrc": sr['SoundRecordingEdition'] if 'SoundRecordingEdition' in sr else None,
            }

    sql = []
    for key, val in sound_recording_map.items():
        sql.append(
            xml_mapper.get_data_from_db(
                db_pool, "'{}',id_track,isrc_track", "tracks", "isrc_track", "'{}'", execute=False
            ).format(key, val['track_isrc']['ResourceId']['ISRC'])
        )
    sql = " UNION ".join(sql).replace(";", " \n ")
    rows1 = connections.execute_query(db_pool, sql, {})

    for r in rows1:
        sound_recording_map[r[0]]['id_track'] = r[1]
        sound_recording_map[r[0]]['isrc_track'] = r[2]

    cons = dict()
    for key, val in sound_recording_map.items():
        if key not in cons:
            cons[key] = set()
        if isinstance(val['contributors'], list):
            for v in val['contributors']:
               cons[key].add(party_list_ref[v['ContributorPartyReference']].replace('"', '\\"').replace("'", "\\'"))
                             #.replace('"', '\\"')
        if isinstance(val['contributors'], dict):
            cons[key].add(party_list_ref[val['contributors']['ContributorPartyReference']].replace('"', '\\"').replace("'", "\\'"))
                          #.replace('"', '\\"'))

    roles = dict()
    for key, val in sound_recording_map.items():
        if key not in roles:
            roles[key] = dict()
        if isinstance(val['contributors'], list):
            for v in val['contributors']:
                roles[key][v['ContributorPartyReference']] = set()
                rol = v['Role']
                if isinstance(rol, list):
                    for r in rol:
                        if isinstance(r, str):
                            roles[key][v['ContributorPartyReference']].add(r)
                        elif isinstance(r, dict):
                            roles[key][v['ContributorPartyReference']].add(r['#text'])
                        elif isinstance(r, list):
                            raise 1
                if isinstance(rol, dict):
                    roles[key][v['ContributorPartyReference']].add(rol['#text'])
                roles[key][v['ContributorPartyReference']] = ",".join(roles[key][v['ContributorPartyReference']])

        if isinstance(val['contributors'], dict):
            o = party_list_ref[val['contributors']['ContributorPartyReference']].replace('"', '\\"').replace("'", "\\'")
            roles[key] = set()
            roles[key].add(o)
                          #.replace('"', '\\"'))


    sql = []
    for key, val in cons.items():
        sql.append(
            xml_mapper.get_data_from_db(
                db_pool, "'{}',id_contri,name_contri", "contributors", "name_contri", {}, execute=False
            ).format(key, "'" + "','".join(val) + "'")
        )
    sql = " UNION ".join(sql).replace(";", " \n ")
    rows2 = connections.execute_query(db_pool, sql, {})
    ref_contr_id = dict()
    ref_contr_name = dict()
    if  len(rows2) > 0:
        for row in rows2:
            ref_contr_id[row[0]] = row[1]
            ref_contr_name[row[0]] = row[2]

    sql_in = list()
    for key, val in sound_recording_map.items():
        if key in ref_contr_name:
            cref = party_list_ref_inverted[ref_contr_name[key]]
            role_name = roles[key][cref] if cref in roles[key] else list(roles[key]).pop()
            sql_tmp = [val['id_track'], ref_contr_id[key], "'{}'".format(role_name), insert_id_message]
            sql_in.append("(" + ",".join([ "{}".format(x) for x in sql_tmp]) + ")")
        else:
            logging.error("KeyError:{} no se encuentra en ref_contr_name".format(key))
    if len(sql_in) > 0:
        sql = "insert into tracks_contributors (id_track, id_contri, contributor_role_track_contri, insert_id_message) values {} " \
               "ON DUPLICATE KEY UPDATE audi_edited_track_contri = CURRENT_TIMESTAMP, update_id_message={};".format(','.join(sql_in), update_id_message)
        res = connections.execute_query(db_pool, sql, {})
    else:
        logging.error("ERROR, posiblemente porque no hay Contributors, no se ejecuto la cquery {}".format(sql))



def upsert_track_contributor(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    upsert_use_track_contributor_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)


