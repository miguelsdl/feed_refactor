from importlib import import_module
import datetime
import xmltodict
import logging
import yaml
from sqlalchemy import text
import os

from sqlalchemy.sql.functions import now
import random
import connections
import xml_album_mapper
import xml_artist_mapper
import xml_genre_mapper
import xml_label_mapper
import xml_use_type_mapper
import xml_comercial_model_type_mapper
import xml_track_mapper2
import xml_contributor_mapper
import xml_rel_album_artist_mapper
import xml_rel_album_genre_mapper
import xml_rel_album_track_mapper
import xml_rel_track_artist_mapper
import xml_rel_albums_rights_mapper
import xml_rel_albums_tracks_rights_mapper
import xml_rel_track_contributor

# Conectar a MySQL
db_pool = connections.get_db_connection_pool('feed_mysql')
db_mongo = connections.get_mongo_client('deliveries')


all_files = [
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/A10301A0003548149X.xml',
    '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/A10301A00006698146_manuel.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00006698146_20241023105524511/A10301A00006698146.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00006698154_20241023105524501/A10301A00006698154.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00007845431_20241023105433727/A10301A00007845431.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0000935334X_20241023105519505/A10301A0000935334X.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00012480823_20241023105323474/A10301A00012480823.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0001320423A_20241023105318511/A10301A0001320423A.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0001324138U_20241023105527505/A10301A0001324138U.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00028356672_20241023105519505/A10301A00028356672.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0002909777O_20241023105318508/A10301A0002909777O.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0002913028M_20241023105506508/A10301A0002913028M.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003194990I_20241023105315491/A10301A0003194990I.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00032841950_20241023105315484/A10301A00032841950.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00032975696_20241023105318511/A10301A00032975696.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003457058F_20241023105412486/A10301A0003457058F.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003476861G_20241023105415502/A10301A0003476861G.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003480299Y_20241023105415497/A10301A0003480299Y.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003481721M_20241023105506512/A10301A0003481721M.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00035091829_20241023105415510/A10301A00035091829.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003509244D_20241023105341522/A10301A0003509244D.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003542592J_20241023105318514/A10301A0003542592J.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003546589F_20241023105503494/A10301A0003546589F.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00035480592_20241023105318517/A10301A00035480592.xml',


    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003548149X_20241023105318511/A10301A0003548149X.xml',


    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00035487029_20241023105308520/A10301A00035487029.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003548934E_20241023105341517/A10301A0003548934E.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003549128R_20241023105506511/A10301A0003549128R.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003552105C_20241023105318509/A10301A0003552105C.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00035523780_20241023105308522/A10301A00035523780.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003556612D_20241023105549520/A10301A0003556612D.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003556613B_20241023105534523/A10301A0003556613B.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003729265G_20241023105318511/A10301A0003729265G.xml',
    # #
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003914291X_20241023105326495/A10301A0003914291X.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003989012I_20241023105527501/A10301A0003989012I.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004249390S_20241023105552530/A10301A0004249390S.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004249391Q_20241023105552517/A10301A0004249391Q.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004268965Z_20241023105549513/A10301A0004268965Z.xml',
    #
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004268966X_20241023105549520/A10301A0004268966X.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004268967V_20241023105549519/A10301A0004268967V.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004498871D_20241023105534524/A10301A0004498871D.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004504073S_20241023105534519/A10301A0004504073S.xml',
    # '/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0004825862V_20241023105315486/A10301A0004825862V.xml',
]


# all_files = ['/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003989012I_20241023105527501/A10301A0003989012I.xml', ]


i = 1
print(datetime.datetime.now())
for f in all_files:
    try:
        file_path = f
        with open(file_path, 'r', encoding='utf-8') as file:
            file_content = file.read()

        # paso el archivo xml a dict
        json_dict = xmltodict.parse(file_content)

        # Cargar el archivo de configuración para la versión DDEX 4.3
        with open('/home/miguel/PycharmProjects/feed_refactor/miguel/src/settings/ddex_43.yaml', 'r') as config_file:
            ddex_map = yaml.safe_load(config_file)
        update_id_message = random.randint(1, 999999)
        insert_id_message = random.randint(1, 999999)
        id_dist = 212321
        # print(datetime.datetime.now())
        upserted_album = xml_album_mapper.upsert_album(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        # #
        # # # print(datetime.datetime.now())
        upserted_artist = xml_artist_mapper.upsert_artist(db_mongo, db_pool, json_dict, ddex_map, upserted_album, update_id_message, insert_id_message)
        # #
        # # # print(datetime.datetime.now())
        # # # Hay diferencia entre mysql y mongo
        upserted_track = xml_track_mapper2.upsert_tracks(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        # #
        # # # print(datetime.datetime.now())
        # # # pronto
        upserted_label = xml_label_mapper.upsert_label(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        #
        # # print(datetime.datetime.now())
        # # pronto
        upserted_genre = xml_genre_mapper.upsert_genre(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        #
        # # print(datetime.datetime.now())
        # # pronto
        upserted_use_type = xml_use_type_mapper.upsert_use_type(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        # #
        # # # print(datetime.datetime.now())
        # # pronto
        upserted_comercial_model_type = xml_comercial_model_type_mapper.upsert_commercial_use_type(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        #
        # print(datetime.datetime.now())
        # pronto
        xml_contributor_mapper.upsert_contributors(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)

        # # print(datetime.datetime.now())
        # # Pronto
        xml_rel_album_artist_mapper.upsert_rel_album_track(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        #
        # # print(datetime.datetime.now())
        # # pronto
        xml_rel_album_genre_mapper.upsert_rel_album_genre(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        #
        # # print(datetime.datetime.now())
        # # PRONTO
        xml_rel_album_track_mapper.upsert_rel_album_track(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, file_path)
        # # #
        # # # # print(datetime.datetime.now())
        xml_rel_albums_rights_mapper.upsert_rel_album_right(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message,id_dist )
        #
        # print(datetime.datetime.now())
        # Falta, hay que revisar unos null
        xml_rel_albums_tracks_rights_mapper.upsert_rel_album_track_right(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message, id_dist)
        # #
        # # # # print(datetime.datetime.now())
        xml_rel_track_artist_mapper.upsert_rel_track_artist_in_db(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        #
        # # print(datetime.datetime.now())
        xml_rel_track_contributor.upsert_track_contributor(db_mongo, db_pool, json_dict, ddex_map, update_id_message, insert_id_message)
        print(" Archivo: ", i, " procesado, path: ", file_path)
        i += 1
    except Exception as error:
        print(file_path)
        raise error
print(datetime.datetime.now())
'/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003140208H_20241023105318508/A10301A0003140208H.xml'
'/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003544416J_20241023105326491/A10301A0003544416J.xml'
'/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A0003544416J_20241023105326491/A10301A0003544416J.xml'
'/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00006698146_20241023105524511/A10301A00006698146.xml'
'/home/miguel/PycharmProjects/feed_refactor/miguel/src/xml/N_A10301A00006698146_20241023105524511/A10301A00006698146.xml'