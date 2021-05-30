# Import libraries
import csv, sys, os, glob
from functools import reduce
from os import system
import pandas as pd, numpy as np
import psycopg2 as psql
from psycopg2 import sql
sys.path.append(os.path.abspath('../')) # Lazy path find
from src.config.config import config # Take in a PostgreSQL table and outputs a pandas dataframe

class postgres_conn:
    """Class for connecting to postgreSQL and applying functions for comparing_urban_population"""
    
    def __init__(self, section, config_path, config_file, country_iso3 = None):
        
        """
        Parameters
        ----------
        
        section       : Database type e.g postgresql
        
        config_path   : Path to config path
        
        config_file   : File containing configuration for postgresql 
        
        country_iso3 = None
                
        """
        
        self.section     = section
        self.config_path = config_path 
        self.config_file = config_file
        self.country_iso3 = country_iso3

    def add_postgis_create_grid_functions(self):

        """
        Apply a function that applies all user defined postgis functions to a postgreSQL database 
        
        Returns
        -------
        A print statement to state that postgis functions have been added to the schema
        """

        # Set conn as None before trying connection
        conn = None
        
        try:
            # Connecting to db using params
            dbParams = config(self.section, self.config_path, self.config_file)
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
            
            # Open sql file and commit query to db
            with conn.cursor() as c:
                # Open sql file 
                sql_file = open('../src/sql/create_grid.sql','r')
                c.execute(sql_file.read())
                conn.commit()
                # Print functions successfully added to postgreSQL
                print('Successfully added postgis functions')
        
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn):
                    c.close()
                    conn.close()
                    print("PostgreSQL connection is closed")
    
    def add_postgis_calc_nation_urban_shares(self):

        """
        Apply a function that applies all user defined postgis functions for calculating national urban shares
        This is split by each gridded population dataset and density threshold
        
        Returns
        -------
        A print statement to state that postgis functions have been added to the schema
        """

        # Set conn as None before trying connection
        conn = None
        
        try:
            # Connecting to db using params
            dbParams = config(self.section, self.config_path, self.config_file)
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
            
            # Open sql file and commit query to db
            with conn.cursor() as c:
                # Open sql file 
                sql_file = open('../src/sql/national_urban_share.sql','r')
                c.execute(sql_file.read())
                conn.commit()
                # Print functions successfully added to postgreSQL
                print('Successfully added postgis functions')
        
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn):
                    c.close()
                    conn.close()
                    print("PostgreSQL connection is closed")
    
    def psql_to_df(self, query = 'SELECT * FROM urban_pop.vendors'):
    
        """ 
        Pull data from postgres database server and save as Pandas DataFrame 
        
        Parameters
        ----------
        
        query    : postgreSQL query sent to database connection
        
        Returns
        -------
        A pandas DataFrame containing results from sql query
        """
        
        # Set conn as None before trying connection
        conn = None

        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        # Open connection
        conn = psql.connect(**dbParams)
            
        df = pd.read_sql(query, con = conn)
        
        conn.close() 
        return(df)
    
    def psql_to_gdf(self, query = 'SELECT * FROM urban_pop.vendors'):
    
        """ 
        Pull data from postgres database server and save as Geopandas GeoDataFrame 
        
        Parameters
        ----------
        
        query    : postgreSQL query sent to database connection
        
        Returns
        -------
        A geopandas GeoDataFrame containing results from sql query
        """
        import geopandas as gpd
        
        # Set conn as None before trying connection
        conn = None
        
        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        # Open connection
        conn = psql.connect(**dbParams)
        
        gdf = gpd.read_postgis(sql = query, con = conn, geom_col='geom')
            
        return gdf

    def gdf_to_postgis(self, gdf, name, schema):
    
        """ 
        gdf to postgis
        
        Parameters
        ----------
        
        gdf     : GeoDataFrame
        
        name    : table name
        
        schema  : schema name
        
        Returns
        -------
        A geopandas GeoDataFrame containing results from sql query
        """
        import geopandas as gpd
        
        # Set conn as None before trying connection
        conn = None
        
        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        # Open connection
        conn = psql.connect(**dbParams)
        
        gdf.to_postgis(name = name, con = conn, schema = schema)
        
    def gpkg_to_psql(self, path = '../data/gadm/gadm36_levels.gpkg', 
                     schema = 'urban_pop', table = 'gadm_level1', s_src = '4326', level = 'level0'):

        """
        Apply a function uploads geopackage layer to a postgis-postgreSQL enabled table
        
                
        Parameters
        ----------
        path     : Path to raster
        
        schema   : Name of schema

        table    : Name of table

        s_src    : Coordinate Reference System of raster 
        
        level    : geopackage layer
        
        Returns
        -------
        A new postgreSQL table containing raster
        """
        
        # Check if file first exists
        if not os.path.exists(path):
            raise ValueError('File does not exist, download geopackage first')
        
        # Define schema.table
        layer = '.'.join([schema, table])

        # Get credentials
        cred = config()
        
        host     = list(cred.values() )[0]
        dbname   = list(cred.values() )[1]
        user     = list(cred.values() )[2]
        pwd      = list(cred.values() )[3]
        port     = list(cred.values() )[4]
        
        # Upload gpkg layer to postgreSQL
        cmd = 'ogr2ogr -f "PostgreSQL" "PG:host=%s user=%s dbname=%s password=%s" %s %s -nlt PROMOTE_TO_MULTI -nln %s -overwrite' \
                %(host, user, dbname, pwd, path, level, layer)
        system(cmd)

    def psql_to_shp(self, file_name = 'temp.shp', file_path = '../data/shp/', 
                    schema = 'urban_pop', table = 'gadm_level1', sql = None):

                """
                Apply a function that pulls postgreSQL table and exports as shapefile
                
                Parameters
                ----------
                file_name  : Name of shapefile
                
                file_path  : Path to shapefile
                
                schema     : Name of schema

                table      : Name of table

                sql        : SQL statement 
                
                Returns
                -------
                A new postgreSQL table containing raster
                """
                
                # Create folder if does not already exist
                if not os.path.exists(file_path):    
                    os.makedirs(file_path)
                # Define file path to where shp will be exported
                path  = file_path + file_name
                # Define schema.tbale
                layer = '.'.join([schema, table])

                # Get credentials
                cred = config()
                
                host     = list(cred.values() )[0]
                dbname   = list(cred.values() )[1]
                user     = list(cred.values() )[2]
                pwd      = list(cred.values() )[3]
                port     = list(cred.values() )[4]
                
                # Upload gpkg layer to postgreSQL
                cmd = 'ogr2ogr -f "ESRI Shapefile" %s "PG:host=%s user=%s dbname=%s password=%s" %s -sql "%s" -overwrite' \
                        %(path, host, user, dbname, pwd, layer, sql)
                system(cmd)

    def psql_to_gpkg(self, file_name = 'temp.gpkg', file_path = '../data/gpkg/', 
                    schema = 'urban_pop', table = 'gadm_level1', sql = None):

                """
                Apply a function that pulls postgreSQL table and exports as gpkg
                
                Parameters
                ----------
                file_name  : Name of gpkg
                
                file_path  : Path to gpkg
                
                schema     : Name of schema

                table      : Name of table

                sql        : SQL statement 
                
                Returns
                -------
                A new postgreSQL table containing raster
                """
                
                # Create folder if does not already exist
                if not os.path.exists(file_path):    
                    os.makedirs(file_path)
                # Define file path to where shp will be exported
                path  = file_path + file_name
                
                # Define schema.tbale
                layer = '.'.join([schema, table])

                # Get credentials
                cred = config()
                
                host     = list(cred.values() )[0]
                dbname   = list(cred.values() )[1]
                user     = list(cred.values() )[2]
                pwd      = list(cred.values() )[3]
                port     = list(cred.values() )[4]
                
                # Upload gpkg layer to postgreSQL
                cmd = 'ogr2ogr -f "GPKG" %s "PG:host=%s user=%s dbname=%s password=%s" %s -sql "%s" -nln gridded -overwrite' \
                        %(path, host, user, dbname, pwd, layer, sql)
                system(cmd)
                
    def raster_to_psql(self, 
                       path = '../data/gpw/gpw_v4_population_density_adjusted_to_2015_unwpp_country_totals_rev11_2015_30_sec.tif', 
                       schema = 'urban_pop', table = 'gpw', s_src = '4326'):

        """
        Apply a function that uploads a raster image to to postgreSQL using
        raster2pgsql command line tool 
        
        Parameters
        ----------
        path     : Path to raster
        
        schema   : Name of schema

        table    : Name of table

        s_src    : Coordinate Reference System of raster 
        
        Returns
        -------
        A new postgreSQL table containing raster
        """
        
        if not os.path.exists(path):
            raise ValueError('File does not exist, download raster first')
        
        layer = '.'.join([schema, table])

        # Get credentials
        cred = config()
        
        host     = list(cred.values() )[0]
        dbname   = list(cred.values() )[1]
        user     = list(cred.values() )[2]
        pwd      = list(cred.values() )[3]
        port     = list(cred.values() )[4]
        
        # Set pg password as env var to suppress psql asking for password
        os.environ['PGPASSWORD'] = pwd 

        # Define command to export each cropped tif to psql
        cmd = 'raster2pgsql -s %s -I -C -t 100x100 %s %s  | psql -h %s -p %s -U %s -d %s'  %(s_src, path, 
                                                                                             layer, 
                                                                                             host, port, 
                                                                                             user, dbname)
        # Execute command
        system(cmd)

    def create_schema(self, schema = 'urban_pop'):
        
        """
        Create postgreSQL schema urban_pop for storing tables 
        """
        
        #global conn
        #global curr
        
        conn = None
        try:
            # Define db_params
            dbParams = config(self.section, self.config_path, self.config_file)
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
            # Define cursor
            curr = conn.cursor()
            # Execute query
            curr.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {};').format(sql.Identifier(schema)))
            # Commit query to database
            conn.commit()
            # Print functions successfully added to postgreSQL
            print('Successfully executed postgreSQL query...')
            
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn) is not None:
                    curr.close()
                    conn.close()
                    print("PostgreSQL connection is closed")

    def drop_schema(self, schema = 'urban_pop'):
        
        """
        Create postgreSQL schema for storing tables 
        """
        
        #global conn
        #global curr
        
        conn = None
        
        try:
            # Define db_params
            dbParams = config(self.section, self.config_path, self.config_file)
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
            # Autocommit query 
            #conn.autocommit = True
            curr = conn.cursor()
            # Execute query
            curr.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE;').format(sql.Identifier(schema)))         
            # Commit query to database
            conn.commit()
            # Print functions successfully added to postgreSQL
            print('Successfully executed postgreSQL query...')
            
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn) is not None:
                    curr.close()
                    conn.close()
                    print("PostgreSQL connection is closed")

    def drop_table(self, schema = 'urban_pop', table = 'table'):
        
        """ Drop postgresSQL insert a new vendor into the vendors table """
        
                
        conn = None
        
        try:
            # Define db_params
            dbParams = config(self.section, config_path, config_file)
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
            # Define cursor
            curr = conn.cursor()
            # Define query 
            model_query = sql.SQL("DROP TABLE IF EXISTS {0}.{1};") 
            # Format model query 
            query = model_query.format(schema, table)
            # execute the query
            curr.execute(DROP_TABLE)
            # commit the changes to the database
            conn.commit()
            # Print functions successfully added to postgreSQL
            print('Successfully executed postgreSQL queries...')

        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
            
        finally:
            # Close all database connections
                if(conn) is not None:
                    curr.close()
                    conn.close()  
     

    def create_grid(self, schema, table):
        
        """ Apply postgis functions to generate gridded vector data for each nation """
                
        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
        
            
        try:
            # Set conn as None before trying connection
            conn = None
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
                
            with conn.cursor() as c:
                
                
                sql_query = sql.SQL('DROP TABLE IF EXISTS {global_grid1}; CREATE TABLE {global_grid2}(fid SERIAL PRIMARY KEY, ' \
                                    'gid_0 TEXT, gid_1 TEXT, uid TEXT, ' \
                                    'gpw_sum NUMERIC, ghs_pop_sum NUMERIC, worldpop_sum NUMERIC, hrsl_sum NUMERIC, ' \
                                    'geom geometry(Polygon, 4326), X NUMERIC, Y NUMERIC );').format(
                                    global_grid1 = sql.SQL('.'.join([schema, table])),
                                    global_grid2 = sql.SQL('.'.join([schema, table])) )
                c.execute(sql_query)
                # Commit query 
                conn.commit()
                # Print functions successfully added to postgreSQL
                print('Successfully created global grid table (if not exists)')
              
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn):
                    c.close()
                    conn.close()
                    print("PostgreSQL connection is closed")                              
        
        for iso in self.country_iso3:

            try:
                # Set conn as None before trying connection
                conn = None
                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                # Open connection
                conn = psql.connect(**dbParams)
                    
                with conn.cursor() as c:
                    
                    # Define nation in quotes for where clause
                    iso_quote = "'%s'" % iso
                    # Call user generated function
                    c.callproc('create_tmp_subdivided_gadm0', ('.'.join([schema, 'tmp_subdivided_gadm0']), iso_quote, '.'.join([schema, 'gadm_level0']) ) ) 
                    # Commit query 
                    conn.commit()
                    # Print functions successfully added to postgreSQL
                    print('Successfully created temporary subdivided gadm table')
                  
            # Return exception if process is unsuccessful 
            except (Exception, psql.DatabaseError) as error :
                print ("Error while running query", error)
            finally:
                # Close all database connections
                    if(conn):
                        c.close()
                        conn.close()
                        print("PostgreSQL connection is closed")  

            try:
                # Set conn as None before trying connection
                conn = None
                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                # Open connection
                conn = psql.connect(**dbParams)
                    
                with conn.cursor() as c:
                    
                    # Define nation in quotes for where clause
                    iso_quote = "'%s'" % iso
                    # Call user generated function
                    c.callproc('create_tmp_subdivided_gadm1', ('.'.join([schema, 'tmp_subdivided_gadm1']), iso_quote, '.'.join([schema, 'gadm_level1']) ) ) 
                    # Commit query 
                    conn.commit()
                    # Print functions successfully added to postgreSQL
                    print('Successfully created temporary subdivided gadm table')
                  
            # Return exception if process is unsuccessful 
            except (Exception, psql.DatabaseError) as error :
                print ("Error while running query", error)
            finally:
                # Close all database connections
                    if(conn):
                        c.close()
                        conn.close()
                        print("PostgreSQL connection is closed") 
                        
            try:
                # Set conn as None before trying connection
                conn = None
                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                # Open connection
                conn = psql.connect(**dbParams)
                    
                with conn.cursor() as c:
                    
                    # Call user generated function
                    c.callproc('create_tmp_grid', ('.'.join([schema, 'tmp_grid']), 
                                                   '.'.join([schema, 'tmp_subdivided_gadm1']), 
                                                   '.'.join([schema, 'gpw_global_2015_30_sec']) ) )
                    # Commit query 
                    conn.commit()
                    # Print functions successfully added to postgreSQL
                    print('Successfully created temporary grid table')
                  
            # Return exception if process is unsuccessful 
            except (Exception, psql.DatabaseError) as error :
                print ("Error while running query", error)
            finally:
                # Close all database connections
                    if(conn):
                        c.close()
                        conn.close()
                        print("PostgreSQL connection is closed")  
            
            try:
                # Set conn as None before trying connection
                conn = None
                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                # Open connection
                conn = psql.connect(**dbParams)
                    
                with conn.cursor() as c:
                    
                    # Call user generated function
                    c.callproc('insert_global_grid', 
                               ('.'.join([schema, 'tmp_grid']), 
                                '.'.join([schema, 'tmp_subdivided_gadm0']), 
                                '.'.join([schema, table]) ) )
                    # Commit query 
                    conn.commit()
                    # Print functions successfully added to postgreSQL
                    print('Successfully inserted geometries into global grid table')
                  
            # Return exception if process is unsuccessful 
            except (Exception, psql.DatabaseError) as error :
                print ("Error while running query", error)
            finally:
                # Close all database connections
                    if(conn):
                        c.close()
                        conn.close()
                        print("PostgreSQL connection is closed")
       

        try:
            # Set conn as None before trying connection
            conn = None
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
                
            with conn.cursor() as c:
                
                # Create unique id
                sql_query = sql.SQL("UPDATE urban_pop.global_grid SET uid = concat_ws('_', gid_0, fid)")
                c.execute(sql_query)
                # Commit query 
                conn.commit()
                # Print functions successfully added to postgreSQL
                print('Successfully updated uid col')
              
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn):
                    c.close()
                    conn.close()
                    print("PostgreSQL connection is closed")                              
