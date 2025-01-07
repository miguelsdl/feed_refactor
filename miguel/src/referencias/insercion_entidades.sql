
-- ALBUMS -------------------------------------

INSERT INTO catalog.albums (id_album, upc_album, name_album, subtitle_album, release_type_album, length_album, tracks_qty_album, 
								release_date_album, active_album, specific_data_album, update_id_message, insert_id_message) 
(

	select * from (
		select 
			   a.id_album as id_album,
		       t.upc_album as upc_album, 
			   max(t.name_album) as name_album, 
			   max(t.subtitle_album) as subtitle_album, 
			   case max(t.tracks_qty_album) when 1 then 'Single' else 'Album' end as release_type_album,
			   SEC_TO_TIME(sum(TIME_TO_SEC(length_resource))) as length_album, 
			   max(t.tracks_qty_album) as tracks_qty_album, 
			   max(t.release_date_album) as release_date_album,
			   0 as active_album,
			   max(t.specific_data_album) as specific_data_album,
			   ${id_message} as update_id_message,
			   ${id_message} as insert_id_message
		from 
		(
			select  
			 r.ICPN as upc_album,
			 r.ReferenceTitle as name_album,
			 r.SubTitle as subtitle_album,
			 r.ReleaseType as release_type_album,
			 releases.get_time_from_string(res.Duration) as length_resource,
			 count(res.id_resource) over (partition by res.id_message) as tracks_qty_album,
			 -- r.OriginalReleaseDate as release_date_album,
			 releases.get_date_from_string(r.OriginalReleaseDate) as release_date_album,
			 true as active_album,
			 JSON_OBJECT("tracks_qty_available_128", SUM(case when 1 = 1 then 1 else 0 end) over (partition by r.id_message, r.id_release))  as specific_data_album
			from releases.releases r
			left join releases.releases r2 on r2.id_message = r.id_message 
			left join releases.resources res on res.id_message = r2.id_message and res.ResourceReference = r2.ReleaseResourceReference
		    left join releases.releases r3 on r3.id_message = r2.id_message and r3.id_release = r2.id_release
			where r.id_message = ${id_message}
			-- and r.ReleaseType in ('Album','Single')
			and r.ICPN is not null
			and res.ISRC is not null
			group by  r.id_message, r.id_release, res.ResourceReference
		) as t
		left join catalog.albums a on a.upc_album = t.upc_album
		group by t.upc_album
	) as t2
	
) ON DUPLICATE KEY UPDATE name_album = t2.name_album, subtitle_album = t2.subtitle_album, release_type_album = t2.release_type_album, length_album = t2.length_album, tracks_qty_album = t2.tracks_qty_album, 
						  release_date_album = t2.release_date_album, /* active_album = t2.active_album, */ specific_data_album = t2.specific_data_album, update_id_message = ${id_message};


-- ARTISTS -------------------------------------


INSERT INTO catalog.artists (id_artist, name_artist, id_parent_artist, active_artist, specific_data_artist, update_id_message, insert_id_message)
(

		select DISTINCT
			-- a.id_artist,
			coalesce(a2.id_artist, a.id_artist) as id_artist,
			p.FullName as name_artist,
			-- a.id_parent_artist as id_parent_artist,
			coalesce(a2.id_parent_artist, a.id_parent_artist, 0) as id_parent_artist,
			true as active_artist,
			JSON_SET(COALESCE(a.specific_data_artist,'{}'), '$.cms_image', false) as specific_data_artist,
	     	${id_message} as update_id_message,
			${id_message} as insert_id_message
		from releases.releases r
		left join releases.releases_display_artists_party_reference apr on apr.id_message = r.id_message and apr.id_release = r.id_release
		left join releases.parties p on p.id_message = r.id_message and p.PartyReference = apr.ArtistPartyReference
		left join catalog.artists a on a.name_artist = p.FullName
				left join catalog.artists_potential_conflicts apc on apc.name_arpocon = p.FullName
				left join catalog.artists a2 on a2.id_artist = apc.default_id_artist_arpocon 
		--				 
		where r.id_message = ${id_message}
		and r.ICPN is not null
		and p.FullName is not null 
		
		UNION
		
	    select DISTINCT 
	    	-- a.id_artist,
			 coalesce(a2.id_artist, a.id_artist) as id_artist,
	    	p.FullName as name_artist, 
	    	-- a.id_parent_artist as id_parent_artist,
			coalesce(a2.id_parent_artist, a.id_parent_artist, 0) as id_parent_artist,
			true as active_artist,
			JSON_SET(COALESCE(a.specific_data_artist,'{}'), '$.cms_image', false) as specific_data_artist,
	     	${id_message} as update_id_message,
			${id_message} as insert_id_message
		from releases.resources r
		left join releases.resources_display_artists_party_reference apr on apr.id_message = r.id_message and apr.id_resource = r.id_resource
		left join releases.parties p on p.id_message = r.id_message and p.PartyReference = apr.ArtistPartyReference
		left join catalog.artists a on a.name_artist = p.FullName
				left join catalog.artists_potential_conflicts apc on apc.name_arpocon = p.FullName
				left join catalog.artists a2 on a2.id_artist = apc.default_id_artist_arpocon 
		where r.id_message = ${id_message}
		and r.ISRC is not null 
		and p.FullName is not null 

) ON DUPLICATE KEY UPDATE active_artist = true, update_id_message = ${id_message};

-- Luego de insertar el artista, actualiza el id_aprent_artist = id_artist, habiendolo flaggeado previamente con un 0 en el TRIGGER BEFORE INSERT
UPDATE catalog.artists
SET id_parent_artist = id_artist
WHERE (insert_id_message = ${id_message} or update_id_message = ${id_message})
AND id_parent_artist = 0;


-- CONTRIBUTORS -------------------------------------


INSERT INTO catalog.contributors (id_contri, name_contri, active_contri, update_id_message, insert_id_message)
(

	select distinct
   		c.id_contri as id_contri,
	    p.FullName as name_contri, 
	 	true as active_contri,
     	${id_message} as update_id_message,
		${id_message} as insert_id_message
	from releases.resources_party_contributors rpc 
	left join releases.parties p on p.id_message = rpc.id_message and p.PartyReference = rpc.ContributorPartyReference
	left join catalog.contributors c on c.name_contri = p.FullName
	where rpc.id_message = ${id_message}
	
) ON DUPLICATE KEY UPDATE active_contri = true, update_id_message = ${id_message};



-- TRACKS -------------------------------------


INSERT INTO catalog.tracks (id_track, isrc_track, name_track, version_track, length_track, explicit_track, 
							     active_track, specific_data_track, update_id_message, insert_id_message)
(	

	select * from (
		select 
			   tr.id_track as id_track,
		       t.isrc_track as isrc_track, 
			   t.name_track as name_track, 
			   t.version_track as version_track, 
			   t.length_track as length_track,
			   t.explicit_track as explicit_track,
			   0 as active_track,
			   t.specific_data_track as specific_data_track,
			   ${id_message} as update_id_message,
			   ${id_message} as insert_id_message
		from 
		(
			select  
			r.ISRC as isrc_track,
            coalesce(r.ReferenceTitle, res.ReferenceTitle) as name_track,
			r.SubTitle as version_track,
			releases.get_time_from_string(res.Duration) as length_track,
			case when rdt.ParentalWarningType = 'Explicit' or r.ParentalWarningType = 'Explicit' then true else false end as explicit_track,
			true as active_track,
			JSON_OBJECT("available_128", true, "available_320", true, "available_preview", true)  as specific_data_track
			from releases.releases r
		    left join releases.resources res on res.id_message = r.id_message and res.ResourceReference = r.ReleaseResourceReference
			left join releases.releases_details_by_territory rdt on rdt.id_message = r.id_message and rdt.id_release = r.id_release
			where r.id_message = ${id_message}
			and r.ISRC is not null
			group by r.id_message, r.id_release
		) as t
		left join catalog.tracks tr on tr.isrc_track = t.isrc_track
		group by t.isrc_track
	) as t2			
	
) ON DUPLICATE KEY UPDATE name_track = t2.name_track, version_track = t2.version_track, length_track = t2.length_track, explicit_track = t2.explicit_track, /* active_track = t2.active_track, */
						  specific_data_track = t2.specific_data_track, update_id_message = ${id_message};




-- GENRES -------------------------------------

INSERT INTO catalog.genres (id_genre, name_genre, active_genre, update_id_message, insert_id_message)
(

select distinct 
	g.id_genre, 
	t.name_genre, 
	t.active_genre,     	
	${id_message} as update_id_message,
	${id_message} as insert_id_message 
from
(
	select distinct
	    Genre as name_genre, 
	 	true as active_genre
	from releases.releases r 
	where r.id_message = ${id_message}
	and r.Genre is not null
) as t		
left join catalog.genres g on g.name_genre = t.name_genre
		
) ON DUPLICATE KEY UPDATE active_genre = true, update_id_message = ${id_message};



-- COMERCIAL MODEL TYPES -------------------------------------

INSERT INTO catalog.comercial_model_types (id_cmt, name_cmt, description_cmt, update_id_message, insert_id_message) 
(

select * from 
(
	select distinct 
		cmt.id_cmt, 
		dt.CommercialModelType as name_cmt,
		null as description_cmt,
		${id_message} as update_id_message, 
		${id_message} as insert_id_message
	from releases.deals_terms dt
	left join catalog.comercial_model_types cmt on cmt.name_cmt = dt.CommercialModelType
	where dt.id_message = ${id_message}
) as t

) ON DUPLICATE KEY UPDATE update_id_message = ${id_message};



-- USE TYPES -------------------------------------

INSERT INTO catalog.use_types (id_use_type, name_use_type, description_use_type, update_id_message, insert_id_message) 
(

select * from 
(
	select distinct 
		ut.id_use_type, 
		dtut.UseType as name_use_type,
		null as description_use_type,
		${id_message} as update_id_message, 
		${id_message} as insert_id_message
	from releases.deals_terms_use_types dtut
	left join catalog.use_types ut on ut.name_use_type = dtut.UseType
	where dtut.id_message = ${id_message}
) as t

) ON DUPLICATE KEY UPDATE update_id_message = ${id_message};



-- LABELS -------------------------------------

INSERT INTO catalog.labels (id_label, name_label, active_label, update_id_message, insert_id_message) 
(

select distinct 	
        l.id_label,
        t.name_label,
        t.active_label,
        ${id_message} as update_id_message, 
	    ${id_message} as insert_id_message
from 
(
	select 
		SUBSTRING(p.FullName, 1, 50) as name_label,
		true as active_label
	from releases.releases rel 
	left join releases.parties p on p.id_message = rel.id_message and p.PartyReference = rel.ReleaseLabelReference
	where rel.id_message = ${id_message}
	and p.FullName is not null
) as t
left join catalog.labels l on l.name_label = t.name_label

) ON DUPLICATE KEY UPDATE active_label = true, update_id_message = ${id_message};


