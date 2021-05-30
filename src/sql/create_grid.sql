CREATE OR REPLACE FUNCTION create_tmp_subdivided_gadm0(subdivided_gadm TEXT,
                                           gid_0 TEXT, gadm TEXT)
                                
/* Apply a function that creates subdivided raster tables to polygons 


Parameters
----------

subdivided_gadm  : Name of temp subdivided gadm grid "urban_pop.tmp_subdivided_gadm"
 
gid_0    	       : ISO (alpha-3) name of country

gadm  		 : Input table  - GADM level 1 e.g. urban_pop.gadm_level1

*/

RETURNS void AS $$
	BEGIN 
    
	EXECUTE 
		'DROP TABLE IF EXISTS '||subdivided_gadm||'';

	EXECUTE 
	'CREATE TABLE '||subdivided_gadm||'(fid SERIAL PRIMARY KEY, 
								 gid_0 TEXT,
								geom geometry(Polygon, 4326))';
	EXECUTE
		'WITH
			-- We DO want to subdivide large poly for faster intersect queries
			subdivided AS(
				SELECT 
                    ST_Subdivide((ST_Dump(geom)).geom, 50) AS geom , gid_0
				FROM 
                    '||gadm||'
				WHERE 
                    gid_0 LIKE '||gid_0||'
				)

		-- Insert into table without spatial index
		-- This prevents re-evaluating bounds for each poly in table each iteration
		INSERT INTO '||subdivided_gadm||'(gid_0, geom)
		SELECT gid_0, geom FROM subdivided';

	EXECUTE
		'CREATE INDEX tmp_subdivided_sid0 ON '||subdivided_gadm||' USING GIST (geom)';
	EXECUTE
		'CLUSTER '||subdivided_gadm||' USING tmp_subdivided_sid0';
	END; $$
	LANGUAGE plpgsql;
    
CREATE OR REPLACE FUNCTION create_tmp_subdivided_gadm1(subdivided_gadm TEXT,
                                           gid_0 TEXT, gadm TEXT)
                                
/* Apply a function that creates subdivided raster tables to polygons 


Parameters
----------

subdivided_gadm  : Name of temp subdivided gadm grid "urban_pop.tmp_subdivided_gadm"
 
gid_0    	       : ISO (alpha-3) name of country

gadm  		 : Input table  - GADM level 1 e.g. urban_pop.gadm_level1

*/

RETURNS void AS $$
	BEGIN 
    
	EXECUTE 
		'DROP TABLE IF EXISTS '||subdivided_gadm||'';

	EXECUTE 
	'CREATE TABLE '||subdivided_gadm||'(fid SERIAL PRIMARY KEY, 
								 gid_0 TEXT, gid_1 TEXT,
								geom geometry(Polygon, 4326))';
	EXECUTE
		'WITH
			-- We DO want to subdivide large poly for faster intersect queries
			subdivided AS(
				SELECT 
                    ST_Subdivide((ST_Dump(geom)).geom, 50) AS geom , gid_0, gid_1
				FROM 
                    '||gadm||'
				WHERE 
                    gid_0 LIKE '||gid_0||'
				)

		-- Insert into table without spatial index
		-- This prevents re-evaluating bounds for each poly in table each iteration
		INSERT INTO '||subdivided_gadm||'(gid_0, gid_1, geom)
		SELECT gid_0, gid_1, geom FROM subdivided';

	EXECUTE
		'CREATE INDEX tmp_subdivided_sid1 ON '||subdivided_gadm||' USING GIST (geom)';
	EXECUTE
		'CLUSTER '||subdivided_gadm||' USING tmp_subdivided_sid1';
	END; $$
	LANGUAGE plpgsql;    
    
    
CREATE OR REPLACE FUNCTION create_tmp_grid(tmp_grid TEXT, 
                                          subdivided_gadm TEXT, 
                                          gpw TEXT)
 /* Apply a function that creates a temp grid table and inserts clipped gpw by nation into table
 
Parameters
----------
tmp_grid           : Name of temp grid "urban_pop.tmp_grid"


subdivided_gadm  : Name of temp subdivided gadm grid "urban_pop.tmp_subdivided_gadm"

in_gpw  		   : Input table  - raster e.g. urban_pop.gpw

*/

RETURNS void AS $$

    DECLARE
    track RECORD;
    
    BEGIN
    DROP TABLE IF EXISTS urban_pop.tmp_grid;
    
    CREATE TABLE urban_pop.tmp_grid(fid SERIAL PRIMARY KEY, geom geometry(Polygon, 4326));
    
    FOR track IN 
        (SELECT DISTINCT gid_1 FROM urban_pop.tmp_subdivided_gadm1)
    LOOP
        WITH 
            buf AS(
                SELECT 
                    gid_1, ST_Buffer((ST_Dump(geom)).geom, 0.01) AS geom 
                FROM 
                    urban_pop.tmp_subdivided_gadm1
                    ),
                    
                    clip AS(
                    SELECT 
                        ST_Clip(ST_Union(r.rast), ST_Union(buf.geom),TRUE ) AS rast 
                    FROM 
                        urban_pop.gpw_global_2015_30_sec AS r
                    JOIN 
                        buf
                    ON
                        ST_Intersects(r.rast, buf.geom)
                    WHERE
                        buf.gid_1 = track.gid_1
                    ),

                    poly AS(
                    SELECT 
                        (ST_PixelAsPolygons(rast)).geom AS geom
                    FROM 
                        clip
                    )
            INSERT INTO urban_pop.tmp_grid(geom)
            SELECT geom FROM poly;
    END LOOP;
    
    RAISE NOTICE 'Created subdivided geoms, now creating spatial index for speedy queries';
    
    CREATE INDEX tmp_grid_sid ON urban_pop.tmp_grid USING GIST (geom);
    CLUSTER urban_pop.tmp_grid USING tmp_grid_sid;
END; $$ 
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_global_grid(tmp_grid TEXT,   
                         subdivided_gadm TEXT,  global_grid TEXT)   
 
/* Apply a function that converts raster tables to polygons    
Parameters  
tmp_grid           : Name of temp grid "urban_pop.tmp_grid"  
subdivided_gadm    : Input table  - subdivided GADM level l  
global_grid       : Out table  - Global grid to store all data  

*/

RETURNS void AS $$      
	BEGIN 		
		WITH  

inter AS( 				
	SELECT                      
		(ST_Dump(ST_Intersection(v1.geom, v2.geom))).geom AS geom,     
		v2.gid_0, v1.fid 				
	FROM                     
		urban_pop.tmp_grid AS v1  			
	JOIN                     
		urban_pop.tmp_subdivided_gadm0 AS v2 
	ON                   
		ST_Intersects(v1.geom, v2.geom)  
	), 			

grid AS ( 		
	SELECT 			
	i.gid_0 AS gid_0,
	(ST_Dump(ST_Union(i.geom))).geom::geometry(Polygon,4326) AS geom
	FROM                      
		inter AS i  				
	GROUP BY                      
		gid_0 , fid		
		) 				 				             

INSERT INTO urban_pop.global_grid(geom, gid_0)             
SELECT geom, gid_0 FROM grid; 

END; 
$$ LANGUAGE plpgsql;  
 
 