import logging
from sqlalchemy import text
import connections
import xml_mapper



def upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message):
    sound_recording_map = dict()
    sound_recording = xml_mapper.get_value_from_path(json_dict, ddex_map['SoundRecording'])
    if not isinstance(sound_recording, list):
        sound_recording = [sound_recording, ]
    # P_LABEL_SDRA_STATION
    party_list = xml_mapper.get_value_from_path(json_dict, ddex_map['PartyList'])
    party_list_ref = xml_mapper.get_party_liat_for_ref(party_list)
    to_del = []
    for k, v in party_list_ref.items():
        if "LABEL" in k:
            to_del.append(k)
    for e in to_del:
        del party_list_ref[e]

    party_list_ref_inverted = {v:k for k,v in party_list_ref.items()}

    for sr in sound_recording:
            sound_recording_map[sr['ResourceReference']] = {
                "artist": sr['DisplayArtist'] if 'DisplayArtist' in sr else None,
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
        if isinstance(val['artist'], list):
            for v in val['artist']:
               cons[key].add(party_list_ref[v['ArtistPartyReference']].replace('"', '\\"').replace("'", "\\'"))
                             #.replace('"', '\\"')
        if isinstance(val['artist'], dict):
            cons[key].add(party_list_ref[val['artist']['ArtistPartyReference']].replace('"', '\\"').replace("'", "\\'"))
                          #.replace('"', '\\"'))

    roles = dict()
    "{'A1': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined,Composer,Lyricist', 'P_ARTIST_295': 'UserDefined', 'P_ARTIST_4788': 'UserDefined', 'P_ARTIST_4908': 'UserDefined', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64126': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64128': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64131': 'UserDefined'}, 'A10': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined,Composer,Lyricist', 'P_ARTIST_4788': 'UserDefined', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64123': 'AssociatedPerformer,UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined'}, 'A11': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_43733': '', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64131': 'UserDefined'}, 'A12': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64151': 'Composer,Lyricist'}, 'A13': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_2756870': 'Composer,Lyricist', 'P_ARTIST_2756871': 'Composer,Lyricist', 'P_ARTIST_2756872': 'Composer,Lyricist', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64131': 'UserDefined'}, 'A14': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64131': 'UserDefined', 'P_ARTIST_64137': 'Composer,Lyricist'}, 'A15': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64131': 'UserDefined', 'P_ARTIST_64139': 'Composer,Lyricist'}, 'A2': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined,Composer,Lyricist', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined'}, 'A3': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_1327278': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64160': 'Composer,Lyricist', 'P_ARTIST_64161': 'Composer,Lyricist'}, 'A4': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_2782385': 'Composer,Lyricist', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64131': 'UserDefined'}, 'A5': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined', 'P_ARTIST_2756870': 'Composer,Lyricist', 'P_ARTIST_2756871': 'Composer,Lyricist', 'P_ARTIST_2756872': 'Composer,Lyricist', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64123': 'AssociatedPerformer,UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined'}, 'A6': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined,Composer,Lyricist', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64123': 'AssociatedPerformer,UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined'}, 'A7': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64137': 'Composer,Lyricist'}, 'A8': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'UserDefined,Composer,Lyricist', 'P_ARTIST_269083': 'Composer,Lyricist', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64123': 'AssociatedPerformer,UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined'}, 'A9': {'P_ARTIST_1286795': 'UserDefined', 'P_ARTIST_17349': 'UserDefined', 'P_ARTIST_22436': 'Composer,Lyricist', 'P_ARTIST_64118': 'UserDefined', 'P_ARTIST_64119': 'UserDefined', 'P_ARTIST_64122': 'UserDefined', 'P_ARTIST_64123': 'UserDefined', 'P_ARTIST_64125': 'UserDefined', 'P_ARTIST_64127': 'UserDefined', 'P_ARTIST_64129': 'UserDefined', 'P_ARTIST_64130': 'UserDefined', 'P_ARTIST_64131': 'UserDefined'}}"
    for key, val in sound_recording_map.items():
        if key not in roles:
            roles[key] = dict()
        if isinstance(val['artist'], list):
            lv = val['artist']
            for v in lv:
                roles[key][v['ArtistPartyReference']] = set()
                rol = v['DisplayArtistRole']
                if isinstance(rol, list):
                    for r in rol:
                        if isinstance(r, str):
                            roles[key][v['ArtistPartyReference']].add(r)
                        elif isinstance(r, dict):
                            roles[key][v['ArtistPartyReference']].add(r['#text'])
                        elif isinstance(r, list):
                            raise 1
                if isinstance(rol, dict):
                    roles[key][v['ArtistPartyReference']].add(rol['#text'])
                if isinstance(rol, str):
                    roles[key][v['ArtistPartyReference']].add(rol)

                roles[key][v['ArtistPartyReference']] = ",".join(roles[key][v['ArtistPartyReference']])

        if isinstance(val['artist'], dict):
            # o = party_list_ref[val['artist']['ArtistPartyReference']].replace('"', '\\"').replace("'", "\\'")
            # roles[key] [val['artist']['ArtistPartyReference']] = set()
            # roles[key][val['artist']['ArtistPartyReference']].add(val['artist']['ArtisticRole'])
            # o = party_list_ref[val['artist']['ArtistPartyReference']].replace('"', '\\"').replace("'", "\\'")
            roles[key][val['artist']['ArtistPartyReference']] = val['artist']['DisplayArtistRole']
            # roles[key][val['artist']['ArtistPartyReference']].add()

    sql = []
    for key, val in cons.items():
        sql.append(
            xml_mapper.get_data_from_db(
                db_pool, "'{}',id_artist,name_artist", "artists", "name_artist", {}, execute=False
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
        cref = party_list_ref_inverted[ref_contr_name[key]]
        if cref in roles[key]:
            sql_tmp = [val['id_track'], ref_contr_id[key],"'{}'".format(roles[key][cref]), insert_id_message]
            sql_in.append("(" + ",".join([ "{}".format(x) for x in sql_tmp]) + ")")
        else:
            print(1)
    sql_values = ','.join(sql_in)
    if not len(sql_values) > 0:
        print(3)

    sql = "insert into tracks_artists (id_track, id_artist, artist_role_track_artist,insert_id_message) values {}"\
          "ON DUPLICATE KEY UPDATE audi_edited_track_artist = CURRENT_TIMESTAMP, update_id_message={};".format(sql_values, update_id_message)
    res = connections.execute_query(db_pool, sql, {})



def upsert_rel_track_artist(db_mongo, db_pool, json_dict, ddex_map):
    upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map)
