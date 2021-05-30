# Import libraries
import sys, os
import psycopg2 as psql
from psycopg2 import sql
from dotenv import find_dotenv # Lazy path find   
sys.path.append(os.path.dirname(find_dotenv())) # Lazy path find
from src.config.config import config 
from src.data.db_conn import *

class zonal_stats():    

    def __init__(self, section, config_path, config_file, country_iso3 = None):
        
        """
        Parameters
        ----------
        
        section       : Db type e.g postgresql
        
        config_path   : Path to config path
        
        config_file   : File containing configuration for postgresql 
        
        country_iso3  : Country name - ISO (Alpha-3 standard)
                
        """
        
        self.section          = section
        self.config_path      = config_path 
        self.config_file      = config_file
        self.country_iso3     = country_iso3
        
    def exact_zonalstats(self, ras_path, vec_path, fid, col, stats, output_csv):
        
        """
        Apply a function that uses os.system to apply exactextract zonal statistics
        
        Parameters
        ----------
        ras_path      : Path of raster file
        
        vec_path      : Path of vector file
        
        fid           : Column identifier
        
        col           : Column name of output zonal stats 
        
        stats         : What zonal stats to apply: see https://github.com/isciences/exactextract
        
        output_csv    : Path of output csv
        
        Returns
        -------
        An output csv containing fid and computed zonal statistics for each polgyon
        
        """
        cmd = "exactextract --raster grid:%s --polygons %s --fid %s --stat %s=%s\(grid\) --output %s" %(ras_path, vec_path, fid, col, stats, output_csv)
        # Apply zonalstatistics
        os.system(cmd)




    def comparing_urban_zonal_stats(self, zonal_path = '../data/zonal/', fid = 'uid', stats = 'sum', gpd_ls = ['gpw', 'ghs_pop', 'worldpop'], 
                                    schema = 'urban_pop', table = 'global_grid'):


        """
        Apply a function that pulls postgreSQL table and exports as shapefile
        
        Parameters
        ----------

        
        Returns
        -------
        A new postgreSQL table containing raster
        """
           
        # Create folder if does not already exist
        if not os.path.exists(zonal_path):    
            os.makedirs(zonal_path)
                        
        for iso in self.country_iso3:
        
            # Define name of temp shp
            file_name = 'temp.gpkg'
            # And file path
            file_path = '../data/gpkg/'
            # Define full path 
            vec_path = ''.join([file_path + file_name])
            
            if os.path.exists(vec_path):
                os.remove(vec_path)
                
            # Join schema and table together
            layer = '.'.join([schema, table])
            # Define sql statement to extract from table e.g. urban_pop.global_grid 
            sql = "SELECT * FROM %s WHERE gid_0 LIKE '%s'" %(layer, iso)
            # Define column name of output zonal stats

            # Define db connection class 
            db_conn = postgres_conn(section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini', country_iso3 = iso)
            # Get vector geometries from postgres and store as temp shp
            #db_conn.psql_to_shp(file_name, file_path, schema, table, sql)
            db_conn.psql_to_gpkg(file_name, file_path, schema, table, sql)
            
            # Define full vector path including layer name
            vec_path = vec_path + '[gridded]'
            
            for gpd in gpd_ls:
                
                col = gpd + '_' + stats
                output_path = '../data/zonal/' + iso + '_' + gpd + '.csv'

                if 'gpw' == gpd:
                    
                    # Define input raster path
                    ras_path = '../data/gpw/cropped/gpw_' + iso + '.tif'
                
                    if not os.path.isfile(output_path):
                        # Apply zonal statistics
                        self.exact_zonalstats(ras_path, vec_path, fid, col, stats, output_csv = output_path)
                       
                # Apply zonal statistics if db is ghs_pop
                elif 'ghs_pop' == gpd:
                    
                    # Define input raster path
                    ras_path = '../data/ghs_pop/cropped/ghs_pop_' + iso + '.tif'

                    if not os.path.isfile(output_path):
                        # Apply zonal statistics
                        self.exact_zonalstats(ras_path, vec_path, fid, col, stats, output_csv = output_path)
        
                # Apply zonal statistics if db is worldpop
                elif 'worldpop' == gpd:
                    
                    # Define input raster path
                    ras_path = '../data/worldpop/MOSAIC_ppp_prj_2015/ppp_prj_2015_' + iso + '.tif'

                    if not os.path.isfile(output_path):
                        # Apply zonal statistics
                        self.exact_zonalstats(ras_path, vec_path, fid, col, stats, output_csv = output_path)