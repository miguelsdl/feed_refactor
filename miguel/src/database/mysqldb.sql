CREATE TABLE feed.albums (
  `id_album` int NOT NULL AUTO_INCREMENT,
  `upc_album` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `name_album` text,
  `subtitle_album` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `release_type_album` varchar(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `length_album` time DEFAULT NULL,
  `tracks_qty_album` int DEFAULT NULL,
  `release_date_album` datetime DEFAULT NULL,
  `active_album` tinyint DEFAULT NULL,
  `specific_data_album` json DEFAULT NULL,
  `audi_edited_album` timestamp NULL DEFAULT NULL,
  `audi_created_album` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_album`),
  KEY `albums_audi_edited_album_IDX` (`audi_edited_album`) USING BTREE,
  KEY `albums_audi_created_album_IDX` (`audi_created_album`) USING BTREE,
  KEY `albums_upc_album_IDX` (`upc_album`) USING BTREE,
  KEY `albums_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `albums_insert_id_message_IDX` (`insert_id_message`) USING BTREE
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.artists (
  `id_artist` int NOT NULL AUTO_INCREMENT,
  `name_artist` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `id_parent_artist` int DEFAULT NULL,
  `active_artist` tinyint DEFAULT NULL,
  `specific_data_artist` json DEFAULT NULL,
  `audi_edited_artist` timestamp NULL DEFAULT NULL,
  `audi_created_artist` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_artist`),
  KEY `artists_audi_edited_artist_IDX` (`audi_edited_artist`) USING BTREE,
  KEY `artists_audi_created_artist_IDX` (`audi_created_artist`) USING BTREE,
  KEY `artists_name_artist_IDX` (`name_artist`(255)) USING BTREE,
  KEY `artists_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `artists_insert_id_message_IDX` (`insert_id_message`) USING BTREE,
  KEY `artists_id_parent_artist_IDX` (`id_parent_artist`) USING BTREE,
  FULLTEXT KEY `name_artist` (`name_artist`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.albums_artists (
  `id_album_artist` int NOT NULL AUTO_INCREMENT,
  `id_album` int DEFAULT NULL,
  `id_artist` int DEFAULT NULL,
  `artist_role_album_artist` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `active_album_artist` tinyint NOT NULL DEFAULT '1',
  `manually_edited_album_artist` tinyint NOT NULL DEFAULT '0',
  `audi_manually_edited_album_artist` timestamp NULL DEFAULT NULL,
  `audi_edited_album_artist` timestamp NULL DEFAULT NULL,
  `audi_created_album_artist` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_album_artist`),
  UNIQUE KEY `albums_artists_UN` (`id_album`,`id_artist`,`artist_role_album_artist`),
  KEY `albums_artists_id_album_IDX` (`id_album`) USING BTREE,
  KEY `albums_artists_id_artist_IDX` (`id_artist`) USING BTREE,
  KEY `albums_artists_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `albums_artists_insert_id_message_IDX` (`insert_id_message`) USING BTREE,
  KEY `albums_artists_audi_edited_album_artist_IDX` (`audi_edited_album_artist`) USING BTREE,
  KEY `albums_artists_audi_created_album_artist_IDX` (`audi_created_album_artist`) USING BTREE
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.tracks (
  `id_track` int NOT NULL AUTO_INCREMENT,
  `isrc_track` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `name_track` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `version_track` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `length_track` time DEFAULT NULL,
  `explicit_track` tinyint DEFAULT NULL,
  `active_track` tinyint DEFAULT NULL,
  `specific_data_track` json DEFAULT NULL,
  `audi_edited_track` timestamp NULL DEFAULT NULL,
  `audi_created_track` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_track`),
  KEY `tracks_audi_edited_track_IDX` (`audi_edited_track`) USING BTREE,
  KEY `tracks_audi_created_track_IDX` (`audi_created_track`) USING BTREE,
  KEY `tracks_isrc_track_IDX` (`isrc_track`) USING BTREE,
  KEY `tracks_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `tracks_insert_id_message_IDX` (`insert_id_message`) USING BTREE
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.albums_tracks (
  `id_album_track` int NOT NULL AUTO_INCREMENT,
  `id_album` int DEFAULT NULL,
  `id_track` int DEFAULT NULL,
  `volume_album_track` int DEFAULT NULL,
  `number_album_track` int DEFAULT NULL,
  `audi_edited_album_track` timestamp NULL DEFAULT NULL,
  `audi_created_album_track` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_album_track`),
  UNIQUE KEY `albums_tracks_UN` (`id_album`,`id_track`),
  KEY `albums_tracks_id_album_IDX` (`id_album`) USING BTREE,
  KEY `albums_tracks_id_track_IDX` (`id_track`) USING BTREE,
  KEY `albums_tracks_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `albums_tracks_insert_id_message_IDX` (`insert_id_message`) USING BTREE,
  KEY `albums_tracks_audi_edited_album_track_IDX` (`audi_edited_album_track`) USING BTREE,
  KEY `albums_tracks_audi_created_album_track_IDX` (`audi_created_album_track`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.tracks_artists (
  `id_track_artist` int NOT NULL AUTO_INCREMENT,
  `id_track` int DEFAULT NULL,
  `id_artist` int DEFAULT NULL,
  `artist_role_track_artist` varchar(100) DEFAULT NULL,
  `audi_edited_track_artist` timestamp NULL DEFAULT NULL,
  `audi_created_track_artist` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  `active_track_artist` tinyint NOT NULL DEFAULT '1',
  `manually_edited_track_artist` tinyint NOT NULL DEFAULT '0',
  `audi_manually_edited_track_artist` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_track_artist`),
  UNIQUE KEY `tracks_artists_UN` (`id_track`,`id_artist`,`artist_role_track_artist`),
  KEY `AK` (`id_track`,`id_artist`) USING BTREE,
  KEY `tracks_artists_id_artist_IDX` (`id_artist`) USING BTREE,
  KEY `tracks_artists_id_track_IDX` (`id_track`) USING BTREE,
  KEY `tracks_artists_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `tracks_artists_insert_id_message_IDX` (`insert_id_message`) USING BTREE,
  KEY `tracks_artists_audi_edited_track_artist_IDX` (`audi_edited_track_artist`) USING BTREE,
  KEY `tracks_artists_audi_created_track_artist_IDX` (`audi_created_track_artist`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.contributors (
  `id_contri` int NOT NULL AUTO_INCREMENT,
  `name_contri` text,
  `active_contri` tinyint DEFAULT NULL,
  `audi_edited_contri` timestamp NULL DEFAULT NULL,
  `audi_created_contri` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_contri`),
  KEY `contributors_name_contri_IDX` (`name_contri`(50)) USING BTREE,
  KEY `contributors_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `contributors_insert_id_message_IDX` (`insert_id_message`) USING BTREE,
  KEY `contributors_audi_edited_contri_IDX` (`audi_edited_contri`) USING BTREE,
  KEY `contributors_audi_created_contri_IDX` (`audi_created_contri`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.tracks_contributors (
  `id_track_contri` int NOT NULL AUTO_INCREMENT,
  `id_track` int DEFAULT NULL,
  `id_contri` int DEFAULT NULL,
  `contributor_role_track_contri` text,
  `contributor_role_type_track_contri` text,
  `audi_edited_track_contri` timestamp NULL DEFAULT NULL,
  `audi_created_track_contri` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_track_contri`),
  UNIQUE KEY `tracks_contributors_UN` (`id_track`,`id_contri`,`contributor_role_track_contri`(200),`contributor_role_type_track_contri`(200)),
  KEY `tracks_contributors_id_track_IDX` (`id_track`) USING BTREE,
  KEY `tracks_contributors_id_contri_IDX` (`id_contri`) USING BTREE,
  KEY `tracks_contributors_id_track_id_contry_IDX` (`id_track`,`id_contri`) USING BTREE,
  KEY `tracks_contributors_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `tracks_contributors_insert_id_message_IDX` (`insert_id_message`) USING BTREE,
  KEY `tracks_contributors_audi_edited_track_contri_IDX` (`audi_edited_track_contri`) USING BTREE,
  KEY `tracks_contributors_audi_created_track_contri_IDX` (`audi_created_track_contri`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.albums_rights (
  `id_albright` bigint NOT NULL AUTO_INCREMENT,
  `id_album` bigint DEFAULT NULL,
  `id_dist` int DEFAULT NULL,
  `id_label` int DEFAULT NULL,
  `id_cmt` int DEFAULT NULL,
  `id_use_type` int DEFAULT NULL,
  `cnty_ids_albright` json DEFAULT NULL,
  `start_date_albright` datetime DEFAULT NULL,
  `end_date_albright` datetime DEFAULT NULL,
  `audi_edited_albright` timestamp NULL DEFAULT NULL,
  `audi_created_albright` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_albright`),
  UNIQUE KEY `albums_rights_UN` (`id_album`,`id_dist`,`id_label`,`id_cmt`,`id_use_type`),
  KEY `albums_rights_id_album_label_IDX` (`id_album`,`id_label`) USING BTREE,
  KEY `albums_rights_id_album_distributor_IDX` (`id_album`,`id_dist`) USING BTREE,
  KEY `albums_rights_update_id_message_IDX` (`update_id_message`) USING BTREE,
  KEY `albums_rights_insert_id_message_IDX` (`insert_id_message`) USING BTREE,
  KEY `albums_rights_audi_edited_albright_IDX` (`audi_edited_albright`) USING BTREE,
  KEY `albums_rights_audi_created_albright_IDX` (`audi_created_albright`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.albums_tracks_rights (
  `id_albtraright` bigint NOT NULL AUTO_INCREMENT,
  `id_album_track` bigint DEFAULT NULL,
  `id_dist` int DEFAULT NULL,
  `id_label` int DEFAULT NULL,
  `id_cmt` int DEFAULT NULL,
  `id_use_type` int DEFAULT NULL,
  `cnty_ids_albtraright` json DEFAULT NULL,
  `start_date_albtraright` datetime DEFAULT NULL,
  `end_date_albtraright` datetime DEFAULT NULL,
  `pline_text_albtraright` text,
  `pline_year_albtraright` text,
  `audi_edited_albtraright` timestamp NULL DEFAULT NULL,
  `audi_created_albtraright` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_albtraright`),
  UNIQUE KEY `albums_tracks_rights_UN` (`id_album_track`,`id_dist`,`id_label`,`id_cmt`,`id_use_type`),
  KEY `albums_tracks_rights_id_album_track_IDX` (`id_album_track`) USING BTREE,
  KEY `albums_tracks_rights_id_album_track_id_dist_IDX` (`id_album_track`,`id_dist`) USING BTREE,
  KEY `albums_tracks_rights_id_album_track_id_label_IDX` (`id_album_track`,`id_label`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.genres (
  `id_genre` int NOT NULL AUTO_INCREMENT,
  `name_genre` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `active_genre` tinyint DEFAULT NULL,
  `audi_edited_genre` timestamp NULL DEFAULT NULL,
  `audi_created_genre` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_genre`),
  KEY `genres_name_genre_IDX` (`name_genre`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.labels (
  `id_label` int NOT NULL AUTO_INCREMENT,
  `name_label` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `active_label` tinyint DEFAULT NULL,
  `audi_edited_label` timestamp NULL DEFAULT NULL,
  `audi_created_label` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_label`),
  KEY `labels_name_label_IDX` (`name_label`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.use_types (
  `id_use_type` int NOT NULL AUTO_INCREMENT,
  `name_use_type` text,
  `description_use_type` text,
  `audi_edited_use_type` timestamp NULL DEFAULT NULL,
  `audi_created_use_type` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int DEFAULT '0',
  `insert_id_message` int DEFAULT '0',
  PRIMARY KEY (`id_use_type`),
  KEY `use_types_name_use_type_IDX` (`name_use_type`(100)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE feed.comercial_model_types (
  `id_cmt` int NOT NULL AUTO_INCREMENT,
  `name_cmt` text,
  `description_cmt` text,
  `audi_edited_cmt` timestamp NULL DEFAULT NULL,
  `audi_created_cmt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_id_message` int NOT NULL DEFAULT '0',
  `insert_id_message` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_cmt`),
  KEY `comercial_model_types_name_cmt_IDX` (`name_cmt`(100)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

