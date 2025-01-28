create table if not exists albums
(
    id_album            int auto_increment
        primary key,
    upc_album           varchar(20)                         null,
    name_album          text                                null,
    subtitle_album      varchar(200)                        null,
    release_type_album  varchar(25)                         null,
    length_album        time                                null,
    tracks_qty_album    int                                 null,
    release_date_album  datetime                            null,
    active_album        tinyint                             null,
    specific_data_album json                                null,
    audi_edited_album   timestamp                           null,
    audi_created_album  timestamp default CURRENT_TIMESTAMP not null,
    update_id_message   int       default 0                 not null,
    insert_id_message   int       default 0                 not null
)
    collate = utf8mb4_unicode_ci;

create index albums_audi_created_album_IDX
    on albums (audi_created_album);

create index albums_audi_edited_album_IDX
    on albums (audi_edited_album);

create index albums_insert_id_message_IDX
    on albums (insert_id_message);

create index albums_upc_album_IDX
    on albums (upc_album);

create index albums_update_id_message_IDX
    on albums (update_id_message);

create table if not exists albums_artists
(
    id_album_artist                   int auto_increment
        primary key,
    id_album                          int                                 null,
    id_artist                         int                                 null,
    artist_role_album_artist          varchar(100)                        null,
    active_album_artist               tinyint   default 1                 not null,
    manually_edited_album_artist      tinyint   default 0                 not null,
    audi_manually_edited_album_artist timestamp                           null,
    audi_edited_album_artist          timestamp                           null,
    audi_created_album_artist         timestamp default CURRENT_TIMESTAMP not null,
    update_id_message                 int       default 0                 not null,
    insert_id_message                 int       default 0                 not null,
    constraint albums_artists_UN
        unique (id_album, id_artist, artist_role_album_artist)
)
    collate = utf8mb4_unicode_ci;

create index albums_artists_audi_created_album_artist_IDX
    on albums_artists (audi_created_album_artist);

create index albums_artists_audi_edited_album_artist_IDX
    on albums_artists (audi_edited_album_artist);

create index albums_artists_id_album_IDX
    on albums_artists (id_album);

create index albums_artists_id_artist_IDX
    on albums_artists (id_artist);

create index albums_artists_insert_id_message_IDX
    on albums_artists (insert_id_message);

create index albums_artists_update_id_message_IDX
    on albums_artists (update_id_message);

create table if not exists albums_genres
(
    id_album_genre           int auto_increment
        primary key,
    id_album                 int                                 null,
    id_genre                 int                                 null,
    audi_edited_album_genre  timestamp                           null,
    audi_created_album_genre timestamp default CURRENT_TIMESTAMP not null,
    update_id_message        int       default 0                 not null,
    insert_id_message        int       default 0                 not null,
    constraint albums_genres_UN
        unique (id_album, id_genre)
)
    collate = utf8mb4_unicode_ci;

create index albums_genres_audi_created_album_genre_IDX
    on albums_genres (audi_created_album_genre);

create index albums_genres_audi_edited_album_genre_IDX
    on albums_genres (audi_edited_album_genre);

create index albums_genres_id_album_IDX
    on albums_genres (id_album);

create index albums_genres_id_genre_IDX
    on albums_genres (id_genre);

create index albums_genres_insert_id_message_IDX
    on albums_genres (insert_id_message);

create index albums_genres_update_id_message_IDX
    on albums_genres (update_id_message);

create table if not exists albums_rights
(
    id_albright           bigint auto_increment
        primary key,
    id_album              bigint                              null,
    id_dist               int                                 null,
    id_label              int                                 null,
    id_cmt                int                                 null,
    id_use_type           int                                 null,
    cnty_ids_albright     json                                null,
    start_date_albright   datetime                            null,
    end_date_albright     datetime                            null,
    audi_edited_albright  timestamp                           null,
    audi_created_albright timestamp default CURRENT_TIMESTAMP not null,
    update_id_message     int       default 0                 not null,
    insert_id_message     int       default 0                 not null,
    constraint albums_rights_UN
        unique (id_album, id_dist, id_label, id_cmt, id_use_type)
)
    collate = utf8mb4_unicode_ci;

create index albums_rights_audi_created_albright_IDX
    on albums_rights (audi_created_albright);

create index albums_rights_audi_edited_albright_IDX
    on albums_rights (audi_edited_albright);

create index albums_rights_id_album_distributor_IDX
    on albums_rights (id_album, id_dist);

create index albums_rights_id_album_label_IDX
    on albums_rights (id_album, id_label);

create index albums_rights_insert_id_message_IDX
    on albums_rights (insert_id_message);

create index albums_rights_update_id_message_IDX
    on albums_rights (update_id_message);

create table if not exists albums_tracks
(
    id_album_track           int auto_increment
        primary key,
    id_album                 int                                 null,
    id_track                 int                                 null,
    volume_album_track       int                                 null,
    number_album_track       int                                 null,
    audi_edited_album_track  timestamp                           null,
    audi_created_album_track timestamp default CURRENT_TIMESTAMP not null,
    update_id_message        int       default 0                 not null,
    insert_id_message        int       default 0                 not null,
    constraint albums_tracks_UN
        unique (id_album, id_track)
)
    collate = utf8mb4_unicode_ci;

create index albums_tracks_audi_created_album_track_IDX
    on albums_tracks (audi_created_album_track);

create index albums_tracks_audi_edited_album_track_IDX
    on albums_tracks (audi_edited_album_track);

create index albums_tracks_id_album_IDX
    on albums_tracks (id_album);

create index albums_tracks_id_track_IDX
    on albums_tracks (id_track);

create index albums_tracks_insert_id_message_IDX
    on albums_tracks (insert_id_message);

create index albums_tracks_update_id_message_IDX
    on albums_tracks (update_id_message);

create table if not exists albums_tracks_rights
(
    id_albtraright           bigint auto_increment
        primary key,
    id_album_track           bigint                              null,
    id_dist                  int                                 null,
    id_label                 int                                 null,
    id_cmt                   int                                 null,
    id_use_type              int                                 null,
    cnty_ids_albtraright     json                                null,
    start_date_albtraright   datetime                            null,
    end_date_albtraright     datetime                            null,
    pline_text_albtraright   text                                null,
    pline_year_albtraright   text                                null,
    audi_edited_albtraright  timestamp                           null,
    audi_created_albtraright timestamp default CURRENT_TIMESTAMP not null,
    update_id_message        int       default 0                 not null,
    insert_id_message        int       default 0                 not null,
    constraint albums_tracks_rights_UN
        unique (id_album_track, id_dist, id_label, id_cmt, id_use_type)
)
    collate = utf8mb4_unicode_ci;

create index albums_tracks_rights_id_album_track_IDX
    on albums_tracks_rights (id_album_track);

create index albums_tracks_rights_id_album_track_id_dist_IDX
    on albums_tracks_rights (id_album_track, id_dist);

create index albums_tracks_rights_id_album_track_id_label_IDX
    on albums_tracks_rights (id_album_track, id_label);

create table if not exists artists
(
    id_artist            int auto_increment
        primary key,
    name_artist          mediumtext                          null,
    id_parent_artist     int                                 null,
    active_artist        tinyint                             null,
    specific_data_artist json                                null,
    audi_edited_artist   timestamp                           null,
    audi_created_artist  timestamp default CURRENT_TIMESTAMP not null,
    update_id_message    int       default 0                 not null,
    insert_id_message    int       default 0                 not null
)
    collate = utf8mb4_unicode_ci;

create index artists_audi_created_artist_IDX
    on artists (audi_created_artist);

create index artists_audi_edited_artist_IDX
    on artists (audi_edited_artist);

create index artists_id_parent_artist_IDX
    on artists (id_parent_artist);

create index artists_insert_id_message_IDX
    on artists (insert_id_message);

create index artists_name_artist_IDX
    on artists (name_artist(255));

create index artists_update_id_message_IDX
    on artists (update_id_message);

create fulltext index name_artist
    on artists (name_artist);

create table if not exists comercial_model_types
(
    id_cmt            int auto_increment
        primary key,
    name_cmt          text                                null,
    description_cmt   text                                null,
    audi_edited_cmt   timestamp                           null,
    audi_created_cmt  timestamp default CURRENT_TIMESTAMP not null,
    update_id_message int       default 0                 not null,
    insert_id_message int       default 0                 not null,
    constraint cmt_name_use_type
        unique (name_cmt(100))
)
    collate = utf8mb4_unicode_ci;

create index comercial_model_types_name_cmt_IDX
    on comercial_model_types (name_cmt(100));

create table if not exists contributors
(
    id_contri           int auto_increment
        primary key,
    name_contri         text                                null,
    active_contri       tinyint                             null,
    audi_edited_contri  timestamp                           null,
    audi_created_contri timestamp default CURRENT_TIMESTAMP not null,
    update_id_message   int       default 0                 not null,
    insert_id_message   int       default 0                 not null,
    constraint constr_contributors
        unique (name_contri(100))
)
    collate = utf8mb4_unicode_ci;

create index contributors_audi_created_contri_IDX
    on contributors (audi_created_contri);

create index contributors_audi_edited_contri_IDX
    on contributors (audi_edited_contri);

create index contributors_insert_id_message_IDX
    on contributors (insert_id_message);

create index contributors_name_contri_IDX
    on contributors (name_contri(50));

create index contributors_update_id_message_IDX
    on contributors (update_id_message);

create table if not exists genres
(
    id_genre           int auto_increment
        primary key,
    name_genre         varchar(100)                        null,
    active_genre       tinyint                             null,
    audi_edited_genre  timestamp                           null,
    audi_created_genre timestamp default CURRENT_TIMESTAMP not null,
    update_id_message  int       default 0                 not null,
    insert_id_message  int       default 0                 not null,
    constraint constr_genre
        unique (name_genre)
)
    collate = utf8mb4_unicode_ci;

create index genres_name_genre_IDX
    on genres (name_genre);

create table if not exists labels
(
    id_label           int auto_increment
        primary key,
    name_label         varchar(50)                         null,
    active_label       tinyint                             null,
    audi_edited_label  timestamp                           null,
    audi_created_label timestamp default CURRENT_TIMESTAMP not null,
    update_id_message  int       default 0                 not null,
    insert_id_message  int       default 0                 not null,
    constraint name_label_idx
        unique (name_label)
)
    collate = utf8mb4_unicode_ci;

create index labels_name_label_IDX
    on labels (name_label);

create table if not exists tracks
(
    id_track            int auto_increment
        primary key,
    isrc_track          varchar(20)                         null,
    name_track          text                                null,
    version_track       text                                null,
    length_track        time                                null,
    explicit_track      tinyint                             null,
    active_track        tinyint                             null,
    specific_data_track json                                null,
    audi_edited_track   timestamp                           null,
    audi_created_track  timestamp default CURRENT_TIMESTAMP not null,
    update_id_message   int       default 0                 not null,
    insert_id_message   int       default 0                 not null,
    constraint constr_isrc_track
        unique (isrc_track)
)
    collate = utf8mb4_unicode_ci;

create index tracks_audi_created_track_IDX
    on tracks (audi_created_track);

create index tracks_audi_edited_track_IDX
    on tracks (audi_edited_track);

create index tracks_insert_id_message_IDX
    on tracks (insert_id_message);

create index tracks_isrc_track_IDX
    on tracks (isrc_track);

create index tracks_update_id_message_IDX
    on tracks (update_id_message);

create table if not exists tracks_artists
(
    id_track_artist                   int auto_increment
        primary key,
    id_track                          int                                 null,
    id_artist                         int                                 null,
    artist_role_track_artist          varchar(100)                        null,
    audi_edited_track_artist          timestamp                           null,
    audi_created_track_artist         timestamp default CURRENT_TIMESTAMP not null,
    update_id_message                 int       default 0                 not null,
    insert_id_message                 int       default 0                 not null,
    active_track_artist               tinyint   default 1                 not null,
    manually_edited_track_artist      tinyint   default 0                 not null,
    audi_manually_edited_track_artist timestamp                           null,
    constraint tracks_artists_UN
        unique (id_track, id_artist, artist_role_track_artist)
)
    collate = utf8mb4_unicode_ci;

create index AK
    on tracks_artists (id_track, id_artist);

create index tracks_artists_audi_created_track_artist_IDX
    on tracks_artists (audi_created_track_artist);

create index tracks_artists_audi_edited_track_artist_IDX
    on tracks_artists (audi_edited_track_artist);

create index tracks_artists_id_artist_IDX
    on tracks_artists (id_artist);

create index tracks_artists_id_track_IDX
    on tracks_artists (id_track);

create index tracks_artists_insert_id_message_IDX
    on tracks_artists (insert_id_message);

create index tracks_artists_update_id_message_IDX
    on tracks_artists (update_id_message);

create table if not exists tracks_contributors
(
    id_track_contri                    int auto_increment
        primary key,
    id_track                           int                                 null,
    id_contri                          int                                 null,
    contributor_role_track_contri      text                                null,
    contributor_role_type_track_contri text                                null,
    audi_edited_track_contri           timestamp                           null,
    audi_created_track_contri          timestamp default CURRENT_TIMESTAMP not null,
    update_id_message                  int       default 0                 not null,
    insert_id_message                  int       default 0                 not null,
    constraint tracks_contributors_UN
        unique (id_track, id_contri, contributor_role_track_contri(200), contributor_role_type_track_contri(200))
)
    collate = utf8mb4_unicode_ci;

create index tracks_contributors_audi_created_track_contri_IDX
    on tracks_contributors (audi_created_track_contri);

create index tracks_contributors_audi_edited_track_contri_IDX
    on tracks_contributors (audi_edited_track_contri);

create index tracks_contributors_id_contri_IDX
    on tracks_contributors (id_contri);

create index tracks_contributors_id_track_IDX
    on tracks_contributors (id_track);

create index tracks_contributors_id_track_id_contry_IDX
    on tracks_contributors (id_track, id_contri);

create index tracks_contributors_insert_id_message_IDX
    on tracks_contributors (insert_id_message);

create index tracks_contributors_update_id_message_IDX
    on tracks_contributors (update_id_message);

create table if not exists use_types
(
    id_use_type           int auto_increment
        primary key,
    name_use_type         text                                null,
    description_use_type  text                                null,
    audi_edited_use_type  timestamp                           null,
    audi_created_use_type timestamp default CURRENT_TIMESTAMP not null,
    update_id_message     int       default 0                 null,
    insert_id_message     int       default 0                 null,
    constraint constr_name_use_type
        unique (name_use_type(100))
)
    collate = utf8mb4_unicode_ci;

create index use_types_name_use_type_IDX
    on use_types (name_use_type(100));

