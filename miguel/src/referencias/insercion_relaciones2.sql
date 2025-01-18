
-- VARIABLES UTILES -------------------------------------

set @id_album = (select max(a.id_album)
					from releases.releases r 
					join catalog.albums a on a.upc_album = r.ICPN
					where id_message = ${id_message} and r.ICPN is not null);


-- ALBUMS ARTISTS -------------------------------------

delete from catalog.albums_artists
where id_album = @id_album;	

INSERT INTO catalog.albums_artists (id_album_artist, id_album, id_artist, artist_role_album_artist, update_id_message, insert_id_message) 
(

	select * from 
	(
	
		select 
		    aa.id_album_artist,
			t.id_album,
			t.id_artist,
			t.artist_role as artist_role_album_artist,
			${id_message} as update_id_message,
			${id_message} as insert_id_message
		from 
		(
		
			select DISTINCT
				al.id_album, 
				-- a.id_artist,
			    coalesce(a2.id_artist, a.id_artist) as id_artist,
		    	case when ard.ArtisticRole = 'UserDefined' then ard.ArtisticRoleUserDefinedValue 
		    	     when ard.ArtisticRole is null then apr.DisplayArtistRole 
		    	     else ard.ArtisticRole end as artist_role
			from releases.releases r
			left join releases.releases_display_artists_party_reference apr on apr.id_message = r.id_message and apr.id_release = r.id_release
			left join releases.releases_display_artists_artistic_roles_details ard on ard.id_message = r.id_message and ard.id_release = r.id_release and ard.id_reldisartpartref = apr.id_reldisartpartref
			left join releases.parties p on p.id_message = r.id_message and p.PartyReference = apr.ArtistPartyReference
			left join catalog.artists a on a.name_artist = p.FullName
			left join catalog.albums al on al.upc_album = r.ICPN
			     left join catalog.artists_potential_conflicts apc on apc.name_arpocon = p.FullName
				 left join catalog.artists a2 on a2.id_artist = apc.default_id_artist_arpocon 
			where r.id_message = ${id_message}
			and r.ICPN is not null
	
		) as t
		left join catalog.albums_artists aa on aa.id_album = t.id_album and aa.id_artist = t.id_artist and aa.artist_role_album_artist = t.artist_role
		
	) as t2
	
) ON DUPLICATE KEY UPDATE update_id_message = ${id_message};


-- ALBUMS GENRES -------------------------------------	
	
INSERT INTO catalog.albums_genres (id_album_genre, id_album, id_genre, update_id_message, insert_id_message) 
(
	
	select 
		ag.id_album_genre,
		t2.id_album as id_album,
		t2.id_genre as id_genre,
		${id_message} as update_id_message,
		${id_message} as insert_id_message
	from 
	(
		select a.id_album, g.id_genre from
		(
			select distinct
    				r.Genre as genre, 
 				r.ICPN as upc
			from releases.releases r 
			where r.id_message = ${id_message}
			and r.Genre is not null
			and r.ICPN is not null
		) as t
		join catalog.albums a on a.upc_album = t.upc 
		join catalog.genres g on g.name_genre = t.genre

	) as t2
	left join catalog.albums_genres ag on ag.id_album = t2.id_album and ag.id_genre = t2.id_genre

) ON DUPLICATE KEY UPDATE update_id_message = ${id_message};



-- ALBUMS TRACKS -------------------------------------	

INSERT INTO catalog.albums_tracks (id_album_track, id_album, id_track, volume_album_track, number_album_track, update_id_message, insert_id_message) 

(

select 
    at.id_album_track,
    t3.id_album,
    t3.id_track,
    t3.volume_album_track,
    t3.number_album_track,
	${id_message} update_id_message,
	${id_message} insert_id_message
from 
(
	select 
		a.id_album,
		t.id_track,
		DENSE_RANK() over (partition by t2.ICPN order by t2.maxResource) as volume_album_track,
		-- t2.SequenceNumber as number_album_track
		coalesce(t2.SequenceNumber, ROW_NUMBER() over ()) as number_album_track
	from (
		select distinct 
			r.ICPN, 
			r2.ISRC,
			rci.SequenceNumber,
			max(rci.ReleaseResourceReference) over (partition by rci.id_message, rci.id_release, rci.Title) as maxResource
		from releases.releases r 
		left join releases.releases r2 on r2.id_message = r.id_message 
		left join releases.releases_resource_group_content_items rci on rci.id_message = r.id_message and rci.ReleaseResourceReference = r2.ReleaseResourceReference
		where r.id_message = ${id_message}
		and r.ICPN is not null
		and r2.ISRC is not null
	) as t2
	join catalog.albums a on a.upc_album = t2.ICPN
	join catalog.tracks t on t.isrc_track = t2.ISRC
) as t3 
left join catalog.albums_tracks at on at.id_album = t3.id_album and at.id_track = t3.id_track
	
	
) ON DUPLICATE KEY UPDATE volume_album_track = t3.volume_album_track, number_album_track = t3.number_album_track, update_id_message = ${id_message};
	


-- TRACKS ARTISTS -------------------------------------

DELETE catalog.tracks_artists
FROM catalog.tracks_artists
INNER JOIN catalog.albums_tracks ON catalog.albums_tracks.id_track = catalog.tracks_artists.id_track
WHERE catalog.albums_tracks.id_album = @id_album; 

INSERT INTO catalog.tracks_artists (id_track_artist, id_track, id_artist, artist_role_track_artist, update_id_message, insert_id_message)
(

select 
    ta.id_track_artist,
    t.id_track,
    t.id_artist,
 	t.artist_role_track_artist,
 	${id_message} update_id_message,
	${id_message} insert_id_message
from 
(

			select DISTINCT
				t.id_track, 
				-- a.id_artist,
		        coalesce(a2.id_artist, a.id_artist) as id_artist, 
		    	case when ard.ArtisticRole = 'UserDefined' then ard.ArtisticRoleUserDefinedValue 
		    	     when ard.ArtisticRole is null then apr.DisplayArtistRole 
		    	     else ard.ArtisticRole end as artist_role_track_artist
			from releases.releases r
			left join releases.releases_display_artists_party_reference apr on apr.id_message = r.id_message and apr.id_release = r.id_release
			left join releases.releases_display_artists_artistic_roles_details ard on ard.id_message = r.id_message and ard.id_release = r.id_release and ard.id_reldisartpartref = apr.id_reldisartpartref
			left join releases.parties p on p.id_message = r.id_message and p.PartyReference = apr.ArtistPartyReference
			left join catalog.artists a on a.name_artist = p.FullName
			left join catalog.tracks t on t.isrc_track = r.ISRC
			     left join catalog.artists_potential_conflicts apc on apc.name_arpocon = p.FullName
				 left join catalog.artists a2 on a2.id_artist = apc.default_id_artist_arpocon 
			where r.id_message = ${id_message}
			and r.ISRC is not null
			and p.FullName is not null
			
			UNION
		
			select DISTINCT
				t.id_track, 
				-- a.id_artist,
		        coalesce(a2.id_artist, a.id_artist) as id_artist,
		    	case when ard.ArtisticRole = 'UserDefined' then ard.ArtisticRoleUserDefinedValue 
		    	     when ard.ArtisticRole is null then apr.DisplayArtistRole 
		    	     else ard.ArtisticRole end as artist_role_track_artist
			from releases.resources r
			left join releases.releases rel on rel.id_message = r.id_message
			left join releases.resources_display_artists_party_reference apr on apr.id_message = r.id_message and apr.id_resource = r.id_resource
			left join releases.resources_display_artists_artistic_roles_details ard on ard.id_message = r.id_message and ard.id_resource = r.id_resource and ard.id_resdisartpartref = apr.id_resdisartpartref
			left join releases.parties p on p.id_message = r.id_message and p.PartyReference = apr.ArtistPartyReference
			left join catalog.artists a on a.name_artist = p.FullName
			left join catalog.tracks t on t.isrc_track = r.ISRC
				 left join catalog.artists_potential_conflicts apc on apc.name_arpocon = p.FullName
				 left join catalog.artists a2 on a2.id_artist = apc.default_id_artist_arpocon 
			where r.id_message = ${id_message}
			and r.ISRC is not null
			and p.FullName is not null
			
) as t
left join catalog.tracks_artists ta on ta.id_artist = t.id_artist and ta.id_track = t.id_track and ta.artist_role_track_artist = t.artist_role_track_artist

) ON DUPLICATE KEY UPDATE update_id_message = ${id_message};


-- TRACKS CONTRIBUTORS -------------------------------------

DELETE catalog.tracks_contributors
FROM catalog.tracks_contributors
INNER JOIN catalog.albums_tracks ON catalog.albums_tracks.id_track = catalog.tracks_contributors.id_track
WHERE catalog.albums_tracks.id_album = @id_album; 

INSERT INTO catalog.tracks_contributors (id_track_contri, id_track, id_contri, contributor_role_track_contri, update_id_message, insert_id_message)
(

select 
    tc.id_track_contri,
    t.id_track,
    t.id_contri,
 	t.contributor_role_track_contri,
 	${id_message} update_id_message,
	${id_message} insert_id_message
from 
(

		select distinct r.ISRC,
		    t.id_track,
	   	    c.id_contri,
	   	 	case rpcr.Role when 'UserDefined' then rpcr.RoleUserDefinedValue else rpcr.Role end as contributor_role_track_contri
		from releases.resources_party_contributors rpc 
	    left join releases.resources r on r.id_resource = rpc.id_resource and r.id_message = rpc.id_message
	    left join releases.resources_party_contributors_roles rpcr on rpcr.id_resource = rpc.id_resource and rpcr.id_message = rpc.id_message and rpcr.id_respartcontri = rpc.id_respartcontri
		left join releases.parties p on p.id_message = rpc.id_message and  p.PartyReference = rpc.ContributorPartyReference
	    left join catalog.contributors c on c.name_contri = p.FullName
	    left join catalog.tracks t on t.isrc_track = r.ISRC
		where rpc.id_message = ${id_message}
		and r.ISRC is not null
		
) as t
left join catalog.tracks_contributors tc on tc.id_contri = t.id_contri and tc.id_track = t.id_track and tc.contributor_role_track_contri = t.contributor_role_track_contri

) ON DUPLICATE KEY UPDATE contributor_role_track_contri = t.contributor_role_track_contri, update_id_message = ${id_message};



-- ALBUM RIGHTS -------------------------------------

delete from catalog.albums_rights 
where id_album = @id_album
and (id_cmt = 0 or id_use_type = 0 or cnty_ids_albright = json_array(0));	


call releases.load_albums_rights_by_id_message_4x(${id_message}, '${distributor_message}');		


-- La siguiente query es la que se genera en el store procedure releases.load_albums_rights_by_id_message_4x

INSERT INTO catalog.albums_rights (id_albright, id_album, id_dist, id_label, id_cmt, id_use_type, cnty_ids_albright, start_date_albright, end_date_albright, update_id_message, insert_id_message) 
(
	select * from
	(
		select
		     ar.id_albright,
		     coalesce(a.id_album, 0) as id_album,
			 coalesce(d.id_dist, 0) as id_dist,
			 coalesce(l.id_label, d.id_dist, 0) as id_label,
			 coalesce(cmt.id_cmt, 0) as id_cmt,
			 coalesce(ut.id_use_type,0 ) as id_use_type,
			 coalesce(JSON_ARRAYAGG(coalesce(co.id_cnty,0))) as cnty_ids_albright, 
			 coalesce(max(t.start_date_albright), date('1900-01-01')) as start_date_albright,
			 null as end_date_albright,
			 p_id_message as update_id_message,
		     p_id_message as insert_id_message
		from 
		(
			select  distinct r.id_release,
				r.ICPN,
				p.FullName as ''DisplayLabelName'',
				dter.CommercialModelType,
				dut.UseType,
				dtt.TerritoryCode,
				releases.get_date_from_string(dter.StartDateTime) as start_date_albright
			from releases r
			left join deals_releases_references rr on rr.id_message = r.id_message and rr.DealReleaseReference = r.ReleaseReference
			left join releases.deals_releases dr on dr.id_message = r.id_message 
			left join deals_terms dter on dter.id_message = r.id_message and dter.id_dealrel = dr.id_dealrel  
			left join deals_terms_use_types dut on dut.id_message = r.id_message and dut.id_dealterm = dter.id_dealterm and dut.id_dealrel = dr.id_dealrel  
			left join deals_terms_territory_codes dtt on dtt.id_message = r.id_message and dtt.id_dealterm = dter.id_dealterm and dtt.id_dealrel = dr.id_dealrel 
			left join releases_details_by_territory rdt on rdt.id_message = r.id_message and rdt.id_release = r.id_release and (rdt.TerritoryCode = dtt.TerritoryCode or rdt.TerritoryCode = 'Worldwide')
			left join parties p on p.id_message = r.id_message and p.PartyReference = r.ReleaseLabelReference
			where r.id_message = p_id_message
            and r.ICPN is not null
			and r.ISRC is null
			and p.FullName is not null
		) as t 
		left join catalog.albums a on a.upc_album = t.ICPN
		left join catalog.use_types ut on ut.name_use_type = t.UseType
		left join catalog.comercial_model_types cmt on cmt.name_cmt = t.CommercialModelType
		left join subscriptions.countries co on co.iso_code_2_cnty = t.TerritoryCode 
		left join catalog.labels l on l.name_label = t.DisplayLabelName 
		left join catalog.distributors d on d.name_dist = p_distributor_message
		left join catalog.albums_rights ar on ar.id_album = a.id_album and ar.id_cmt = cmt.id_cmt and ar.id_use_type = ut.id_use_type
		group by a.id_album, d.id_dist, l.id_label, cmt.id_cmt, ut.id_use_type
	) as t2
) ON DUPLICATE KEY UPDATE id_label = t2.id_label, cnty_ids_albright = t2.cnty_ids_albright, start_date_albright = t2.start_date_albright, end_date_albright = t2.end_date_albright, update_id_message = p_id_message;




-- ALBUMS TRACK RIGHTS -------------------------------------	

DELETE catalog.albums_tracks_rights
FROM catalog.albums_tracks_rights
INNER JOIN catalog.albums_tracks ON catalog.albums_tracks.id_album_track = catalog.albums_tracks_rights.id_album_track
INNER JOIN catalog.albums a on catalog.albums_tracks.id_album = a.id_album
WHERE catalog.albums_tracks.id_album = @id_album
and (id_cmt = 0 or id_use_type = 0 or cnty_ids_albtraright = json_array(0));

call releases.load_albums_tracks_rights_by_id_message_4x(${id_message}, '${distributor_message}');



-- La siguiente query es la que se genera en el store procedure releases.load_albums_tracks_rights_by_id_message_4x

IINSERT INTO catalog.albums_tracks_rights (id_albtraright, id_album_track, id_dist, id_label, id_cmt, id_use_type, cnty_ids_albtraright, start_date_albtraright, end_date_albtraright, 
												   pline_text_albtraright, pline_year_albtraright, update_id_message, insert_id_message) 
	(	
		select * from
		(
		
			select
			     atr.id_albtraright,
			     coalesce(at.id_album_track, 0) as id_album_track,
				 coalesce(d.id_dist, 0) as id_dist,
				 coalesce(l.id_label, d.id_dist, 0) as id_label,
				 coalesce(cmt.id_cmt, 0) as id_cmt,
				 coalesce(ut.id_use_type,0 ) as id_use_type,
				 coalesce(JSON_ARRAYAGG(coalesce(co.id_cnty,0))) as cnty_ids_albtraright, 
				 coalesce(max(t.start_date_albright), date('1900-01-01')) as start_date_albtraright,
				 null as end_date_albtraright,
			     max(t.p_text) as pline_text_albtraright,
				 max(t.p_year) as pline_year_albtraright,
				 p_id_message as update_id_message,
			     p_id_message as insert_id_message 
			from 
			(
			
				select  distinct r.id_release,
					r.ISRC,
					r2.ICPN,
					p.FullName as DisplayLabelName,
					dter.CommercialModelType,
					dut.UseType,
					dtt.TerritoryCode,
	                releases.get_date_from_string(dter.StartDateTime) as start_date_albright,
					rpd.PLineText as p_text,
					rpd.Year as p_year 
				from releases r
				left join releases r2 on r2.id_message = r.id_message 
				left join deals_releases_references rr on rr.id_message = r.id_message and rr.DealReleaseReference = r.ReleaseReference
				left join deals_terms dter on dter.id_message = r.id_message and dter.id_dealrel = rr.id_dealrel  
				left join deals_terms_use_types dut on dut.id_message = r.id_message and dut.id_dealterm = dter.id_dealterm and dut.id_dealrel = rr.id_dealrel  
				left join deals_terms_territory_codes dtt on dtt.id_message = r.id_message and dtt.id_dealterm = dter.id_dealterm and dtt.id_dealrel = rr.id_dealrel 
				left join releases_details_by_territory rdt on rdt.id_message = r.id_message and rdt.id_release = r.id_release and (rdt.TerritoryCode = dtt.TerritoryCode or rdt.TerritoryCode = 'Worldwide')
				left join parties p on p.id_message = r.id_message and p.PartyReference = r.ReleaseLabelReference
			    left join resources res on res.id_message = r.id_message and res.ISRC = r.ISRC
			    left join resources_pline_details rpd on rpd.id_message = r2.id_message and rpd.id_resource = res.id_resource 
				where r.id_message = p_id_message
				and r.ISRC is not null
	            and r2.ICPN is not null
				and r2.ISRC is null	
				and rpd.IsDefaultPLine = 'true'
				
			) as t 
			left join catalog.albums a on a.upc_album = t.ICPN
			left join catalog.tracks tr on tr.isrc_track = t.ISRC
		    left join catalog.albums_tracks at on at.id_track = tr.id_track and at.id_album = a.id_album
			left join catalog.use_types ut on ut.name_use_type = t.UseType
			left join catalog.comercial_model_types cmt on cmt.name_cmt = t.CommercialModelType
			left join subscriptions.countries co on co.iso_code_2_cnty = t.TerritoryCode 
			left join catalog.labels l on l.name_label = t.DisplayLabelName 
			left join catalog.distributors d on d.name_dist = p_distributor_message 
			left join catalog.albums_tracks_rights atr on atr.id_album_track = at.id_album_track and atr.id_cmt = cmt.id_cmt and atr.id_use_type = ut.id_use_type
			where t.TerritoryCode is not null
		    group by at.id_album_track, d.id_dist, l.id_label, cmt.id_cmt, ut.id_use_type
		    
		) as t2

    ) ON DUPLICATE KEY UPDATE id_label = t2.id_label, cnty_ids_albtraright = t2.cnty_ids_albtraright, start_date_albtraright = t2.start_date_albtraright, end_date_albtraright = t2.end_date_albtraright, pline_text_albtraright = t2.pline_text_albtraright, pline_year_albtraright = t2.pline_year_albtraright, update_id_message = p_id_message;





-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Insert en catalog.albums_potential_conflicts si es que encuentra un artista del album señalado en la artists_potential_conflicts
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

insert into catalog.albums_potential_conflicts (id_arpocon, id_album, inducted_id_artist_albpocon, assigned_id_artist_albpocon, status_albpocon)
select apc.id_arpocon, al.id_album, apc.default_id_artist_arpocon, apc.default_id_artist_arpocon, 'open'
			from releases.releases r
			left join releases.releases_display_artists_party_reference apr on apr.id_message = r.id_message and apr.id_release = r.id_release
			left join releases.releases_display_artists_artistic_roles_details ard on ard.id_message = r.id_message and ard.id_release = r.id_release and ard.id_reldisartpartref = apr.id_reldisartpartref
			left join releases.parties p on p.id_message = r.id_message and p.PartyReference = apr.ArtistPartyReference
left join catalog.albums al on al.upc_album = r.ICPN
left join catalog.artists_potential_conflicts apc on apc.name_arpocon = p.FullName
where r.id_message = ${id_message}
and apc.id_arpocon is not null
and al.id_album is not null
and not exists (select 1 from catalog.albums_potential_conflicts where id_album = al.id_album and inducted_id_artist_albpocon = apc.default_id_artist_arpocon and status_albpocon = 'open');








INSERT INTO catalog.albums_tracks_rights (id_albtraright, id_album_track, id_dist, id_label, id_cmt, id_use_type, cnty_ids_albtraright, start_date_albtraright, end_date_albtraright, 
												   pline_text_albtraright, pline_year_albtraright, update_id_message, insert_id_message) 
	(	
		select * from
		(
		
			select
			     atr.id_albtraright,
			     coalesce(at.id_album_track, 0) as id_album_track,
				 coalesce(d.id_dist, 0) as id_dist,
				 coalesce(l.id_label, d.id_dist, 0) as id_label,
				 coalesce(cmt.id_cmt, 0) as id_cmt,
				 coalesce(ut.id_use_type,0 ) as id_use_type,
				 coalesce(JSON_ARRAYAGG(coalesce(co.id_cnty,0))) as cnty_ids_albtraright, 
				 coalesce(max(t.start_date_albright), date('1900-01-01')) as start_date_albtraright,
				 null as end_date_albtraright,
			     max(t.p_text) as pline_text_albtraright,
				 max(t.p_year) as pline_year_albtraright,
				 p_id_message as update_id_message,
			     p_id_message as insert_id_message 
			from 
			(
			
				select  distinct r.id_release,
					r.ISRC,
					r2.ICPN,
					p.FullName as DisplayLabelName,
					dter.CommercialModelType,
					dut.UseType,
					dtt.TerritoryCode,
	                releases.get_date_from_string(dter.StartDateTime) as start_date_albright,
					rpd.PLineText as p_text,
					rpd.Year as p_year 
				from releases r
				left join releases r2 on r2.id_message = r.id_message 
				left join deals_releases_references rr on rr.id_message = r.id_message and rr.DealReleaseReference = r.ReleaseReference
				left join deals_terms dter on dter.id_message = r.id_message and dter.id_dealrel = rr.id_dealrel  
				left join deals_terms_use_types dut on dut.id_message = r.id_message and dut.id_dealterm = dter.id_dealterm and dut.id_dealrel = rr.id_dealrel  
				left join deals_terms_territory_codes dtt on dtt.id_message = r.id_message and dtt.id_dealterm = dter.id_dealterm and dtt.id_dealrel = rr.id_dealrel 
				left join releases_details_by_territory rdt on rdt.id_message = r.id_message and rdt.id_release = r.id_release and (rdt.TerritoryCode = dtt.TerritoryCode or rdt.TerritoryCode = 'Worldwide')
				left join parties p on p.id_message = r.id_message and p.PartyReference = r.ReleaseLabelReference
			    left join resources res on res.id_message = r.id_message and res.ISRC = r.ISRC
			    left join resources_pline_details rpd on rpd.id_message = r2.id_message and rpd.id_resource = res.id_resource 
				where r.id_message = p_id_message
				and r.ISRC is not null
	            and r2.ICPN is not null
				and r2.ISRC is null	
				and rpd.IsDefaultPLine = 'true'
				
			) as t 
			left join catalog.albums a on a.upc_album = t.ICPN
			left join catalog.tracks tr on tr.isrc_track = t.ISRC
		    left join catalog.albums_tracks at on at.id_track = tr.id_track and at.id_album = a.id_album
			left join catalog.use_types ut on ut.name_use_type = t.UseType
			left join catalog.comercial_model_types cmt on cmt.name_cmt = t.CommercialModelType
			left join subscriptions.countries co on co.iso_code_2_cnty = t.TerritoryCode 
			left join catalog.labels l on l.name_label = t.DisplayLabelName 
			left join catalog.distributors d on d.name_dist = p_distributor_message 
			left join catalog.albums_tracks_rights atr on atr.id_album_track = at.id_album_track and atr.id_cmt = cmt.id_cmt and atr.id_use_type = ut.id_use_type
			where t.TerritoryCode is not null
		    group by at.id_album_track, d.id_dist, l.id_label, cmt.id_cmt, ut.id_use_type
		    
		) as t2

    ) ON DUPLICATE KEY UPDATE id_label = t2.id_label, cnty_ids_albtraright = t2.cnty_ids_albtraright, start_date_albtraright = t2.start_date_albtraright, end_date_albtraright = t2.end_date_albtraright, pline_text_albtraright = t2.pline_text_albtraright, pline_year_albtraright = t2.pline_year_albtraright, update_id_message = p_id_message;





