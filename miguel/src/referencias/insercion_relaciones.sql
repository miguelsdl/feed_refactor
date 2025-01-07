
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


-- ALBUMS TRACK RIGHTS -------------------------------------	

DELETE catalog.albums_tracks_rights
FROM catalog.albums_tracks_rights
INNER JOIN catalog.albums_tracks ON catalog.albums_tracks.id_album_track = catalog.albums_tracks_rights.id_album_track
INNER JOIN catalog.albums a on catalog.albums_tracks.id_album = a.id_album
WHERE catalog.albums_tracks.id_album = @id_album
and (id_cmt = 0 or id_use_type = 0 or cnty_ids_albtraright = json_array(0));

call releases.load_albums_tracks_rights_by_id_message_4x(${id_message}, '${distributor_message}');



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


