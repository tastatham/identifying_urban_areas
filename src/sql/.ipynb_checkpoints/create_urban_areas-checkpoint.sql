DROP FUNCTION IF EXISTS create_urban_areas;
CREATE FUNCTION create_urban_areas(nation TEXT, cluster TEXT,
							in_table_name1 TEXT, in_table_name2 TEXT, out_table_name TEXT, 
							fid TEXT, col TEXT, geom TEXT)
/*
Parameters
----------

nation    	    : Single nation to apply analysis

cluster         : Name of cluster name 

in_table_name1  : Input table  - clustered geom

in_table_name2  : Input table  - clustered table 

out_table_name1 : Output table - urban areas

fid       	    : Name of id column

col             : Column name containg raster value

geom 		    : Name of geometry column

s_srs 		    : Source crs (default:4326)
 
sid_name 	    : Spatial index name

*/

RETURNS void AS $$
	BEGIN 
	EXECUTE
		'WITH 
			--Dissolve by clustered groups 
			cluster AS(
					SELECT
					St_Buffer((St_Dump(
					St_Union(St_Buffer(geom, 0.0000000001)))).geom, -0.0000000001) AS geom
					FROM '||in_table_name1||'
					GROUP BY '||col||'
					HAVING '||col||' >= 0 
				),
			--Dump any holes
			noholes AS(
					SELECT (ST_Dump(ST_Collect(
					ST_MakePolygon('||geom||')))).'||geom||' AS '||geom||'
					FROM(
					SELECT ST_ExteriorRing((ST_Dump('||geom||')).'||geom||') AS '||geom||'
					FROM cluster
					) s
					GROUP BY '||geom||'
				),
			--Join population data back 
			joins AS(
					SELECT 
					a.'||geom||' AS '||geom||',
					p.ghsl_sum AS ghsl_sum, 
					p.gpw_sum AS gpw_sum, 
					p.hrsl_sum AS hrsl_sum,  
					p.worldpop_sum AS worldpop_sum,
					'||nation||' AS nation,
					'||cluster||' AS cluster
					FROM
					noholes AS a
					LEFT JOIN '||in_table_name2||' AS p
					ON ST_Intersects(a.'||geom||', p.'||geom||')
				),
				
			groupby AS(
					SELECT 
					(ST_X(ST_Centroid(a.geom))) AS x,
					(ST_Y(ST_Centroid(a.geom))) AS y,
					a.geom AS geom,
					SUM(a.ghsl_sum) AS ghsl_sum, 
					SUM(a.gpw_sum) AS gpw_sum, 
					SUM(a.hrsl_sum) AS hrsl_sum,  
					SUM(a.worldpop_sum) AS worldpop_sum,
					nation,
					cluster
					FROM joins as a
					GROUP BY a.'||geom||', cluster, nation
			
			)
				
		INSERT INTO '||out_table_name||' (x,y,'||geom||',ghsl_sum,gpw_sum,hrsl_sum,worldpop_sum,nation,cluster)
		SELECT x,y,'||geom||',ghsl_sum,gpw_sum,hrsl_sum,worldpop_sum,nation,cluster FROM groupby';

    --Add spatial index after inserting all data to avoid having to rebuild index on every insertion
	END; $$
	LANGUAGE plpgsql;
