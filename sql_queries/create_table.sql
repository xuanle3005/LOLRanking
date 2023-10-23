CREATE OR REPLACE VIEW lolesport.region_match AS
	select DISTINCT blue_team_id as team_id, tournament_slug
	FROM lolesport.teams t, lolesport.tournaments tm
	WHERE t.team_id = tm.blue_team_id
	AND tournament_slug NOT LIKE '%msi%' 
    AND tournament_slug NOT LIKE '%worlds%' 
    AND tournament_slug NOT LIKE '%midseason_cup%'
	UNION 
	select DISTINCT red_team_id as team_id, tournament_slug
	FROM lolesport.teams t, lolesport.tournaments tm
	WHERE t.team_id = tm.red_team_id
	AND tournament_slug NOT LIKE '%msi%' 
    AND tournament_slug NOT LIKE '%worlds%' 
    AND tournament_slug NOT LIKE '%midseason_cup%';

select * from lolesport.region_match;

CREATE TABLE lolesport.update_team AS
	SELECT t.team_id, t.team_name, t.team_slug, t.team_acronym, t.region, r.tournament_slug from lolesport.temp t
	JOIN lolesport.region_match r ON t.team_id = r.team_id
    GROUP BY team_id;
    
drop table lolesport.update_team;
select * from lolesport.update_team where team_name = 'Clutch Gaming Academy';

select count(*) from lolesport.update_team;

SELECT t1.team_name
FROM lolesport.teams t1
LEFT JOIN lolesport.update_team t2 ON t2.team_name = t1.team_name
WHERE t2.team_name IS NULL

SET SQL_SAFE_UPDATES = 0;
UPDATE lolesport.update_team 
SET region = CASE WHEN tournament_slug like '%lck%' 
					and tournament_slug not like '%challenger%'
 					and tournament_slug not like '%academy%'
					or lower(team_name) like '%lck%' then 'lck'
			WHEN tournament_slug like '%lpl%' or lower(team_name) like '%lpl%' then 'lpl'
            WHEN tournament_slug like '%lcs%' 
					and tournament_slug not like '%proving%'
					and tournament_slug not like '%amateur%'

 					and tournament_slug not like '%academy%'
				    or lower(team_name) like '%lcs%' then 'lcs'
		    WHEN tournament_slug like '%lec%' or lower(team_name) like '%lec%' then 'lec'
            ELSE 'others'
            END;
           