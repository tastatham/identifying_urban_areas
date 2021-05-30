import os, zipfile
from os import system, path, makedirs
from functools import reduce
from io import StringIO
import geopandas as gpd
from src.data.db_conn import *

class data_download:
    
    """
    Download gridded population data & gadm boundaries crop using nation extent if needed
    """
        
    def __init__(self, section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini', country_iso3 = None):
        """
        Parameters
        ----------
        
        section       : Database type e.g postgresql
        
        config_path   : Path to config path
        
        config_file   : File containing configuration for postgresql 
        
        country_iso3  : List of countries by alpha-3 code
                
        """
        
        self.section      = section
        self.config_path  = config_path 
        self.config_file  = config_file
        self.country_iso3 = country_iso3
    

    def gadm_boundaries(self):
    
        """
        Apply a function that downloads gadm boundary data in geopackage format
        
        Parameters
        ----------
        None    : N/A

        Returns
        -------
        A geopackage containing nation boundaries
        """
        
        
        # Define folder to create 
        dirs = '../data/gadm/'
        
        # Create folder if does not already exist
        if not os.path.exists(dirs):    
            os.makedirs(dirs)
        # Download GPW if does not exist in path  
        if not os.path.isfile(dirs + 'gadm36_levels_gpkg.zip'):
            system('wget -O ' + dirs + 'gadm36_levels_gpkg.zip https://biogeo.ucdavis.edu/data/gadm3.6/gadm36_levels_gpkg.zip')
        #if os.path.isfile(dirs + 'gadm36_levels_gpkg.zip'):
        
            with zipfile.ZipFile(dirs + 'gadm36_levels_gpkg.zip', 'r') as zip_ref:
                zip_ref.extractall(dirs)
       
        else: # Dataset already exists
            print('gadm boundaries already exists in file path and extracted.. Continue')
            
    def gpw(self):
        
        """
        Apply a function that downloads UN-adjusted Gridded Population of the World v4 from personal dropbox
                
        Parameters
        ----------
        None    : N/A

        Returns
        -------
        tif file containing GPW population counts, in Mercator projection (EPSG:4326)
        """
        
        # Define folder to create 
        dirs = '../data/gpw'
        # Create folder if does not already exist
        if not os.path.exists(dirs):    
            os.makedirs(dirs)

        # Download GPW if does not exist in path  
        if not os.path.isfile(dirs + '/gpw.zip'):
            system('wget -O ' + dirs + '/gpw.zip https://www.dropbox.com/s/mf1982yra7m3i4a/' + \
               'gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11_2015_30_sec_tif.zip' + \
               '--retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 0 >/dev/null 2>&1')
            
            with zipfile.ZipFile(dirs + '/gpw.zip', 'r') as zip_ref:
                zip_ref.extractall(dirs)
                
        else: # Dataset already exists
            print('GPWv4 already exists in file path.. Continue')

    def ghs_pop(self):
        
        """
        Apply a function that downloads UN-adjusted GHS-POP population data
                
        Parameters
        ----------
        None    : N/A

        Returns
        -------
        tif file containing GHS-POP population counts, in Mercator projection (EPSG:4326)
        """
        
        # Define folder to create 
        dirs = '../data/ghs_pop'
        # Create folder if does not already exist
        if not os.path.exists(dirs):    
            os.makedirs(dirs)
            
        # Download GPW if does not exist in path  
        if not os.path.isfile(dirs + '/ghs_pop.zip'):
            system('wget -O ' + dirs + '/ghs_pop.zip https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/' + \
                   'GHS_POP_MT_GLOBE_R2019A/GHS_POP_E2015_GLOBE_R2019A_4326_30ss/V1-0/GHS_POP_E2015_GLOBE_R2019A_4326_30ss_V1_0.zip')
            with zipfile.ZipFile(dirs + '/ghs_pop.zip', 'r') as zip_ref:
                zip_ref.extractall(dirs)
                
        else: # Dataset already exists
            print('GHS-POP already exists in file path.. Continue')

    def worldpop(self):
        
        """
        Apply a function that downloads UN-adjusted WorldPop population data
                
        Parameters
        ----------
        None    : N/A

        Returns
        -------
        tif file containing WorldPoppopulation counts, in Mercator projection (EPSG:4326)
        """
        
        # Define folder to create 
        dirs = '../data/worldpop'
        # Create folder if does not already exist
        if not os.path.exists(dirs):    
            os.makedirs(dirs)
            
        # Download GPW if does not exist in path  
        if not os.path.isfile(dirs + '/global_mosaic_ppp_100m_2015_vrt.zip'):
            system('wget -O ' + dirs + '/global_mosaic_ppp_100m_2015_vrt.zip ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2015/0_Mosaicked/global_mosaic_ppp_100m_2015_vrt.zip')
            with zipfile.ZipFile(dirs + '/global_mosaic_ppp_100m_2015_vrt.zip', 'r') as zip_ref:
                zip_ref.extractall(dirs)
                
        else: # Dataset already exists
            print('Global WorldPop GPD (100m) already exists in file path.. Continue')

    def crop_gpd(self, gpd, schema, table):
        
            """
            Apply a function that crops gridded population datasets by extent of country.
            The preferred way is to present the country in ISO standard to avoid spelling errors but 
            country names in English can also be presented.
            This should only be applied to the GPW and GHS-POP
            
            Parameters
            ----------
            gpd       : Name of gridded population database
            
            schema   : Name of database schema 
            
            Table    : Name of database table containing geometries e.g. global grid
            
            Returns
            -------
            Population databases by nation in utm projection
            """
            
            # Define folder to create 
            dirs = '../data/' + str(gpd) 
            # Create folder if does not already exist
            if not os.path.exists(dirs):    
                os.makedirs(dirs)
            
                # Define class for dealing with db connections
            db_conn = postgres_conn(self.section, self.config_path, self.config_file, self.country_iso3)

            if self.country_iso3 is not None:

                # Loop through list of nations
                for iso in self.country_iso3:
                    # Define sql query for extracting bounds of nation
                    query = sql.SQL('SELECT min(ST_XMin(geom)) as xmin, min(ST_YMin(geom)) as ymin, max(ST_XMax(geom)) as xmax, max(ST_YMax(geom)) as ymax ' \
                                    'FROM {schema}.{table} ' \
                                    'WHERE gid_0 = {country_iso3}').format(
                        schema       = sql.Identifier(schema), 
                        table        = sql.Identifier(table), 
                        country_iso3 = sql.Literal(iso) )
                    
                    # Get extents as input for gdalwarp
                    df = db_conn.psql_to_df(query)
                    xmin, ymin, xmax, ymax = df.squeeze()
                    
                    # Define output path of cliped raster
                    out_path = dirs + '/cropped/'
                    out_ras  = out_path  + str(gpd) + '_' + iso + '.tif'
                    
                    # Create folder if does not already exist
                    if not os.path.exists(out_path):    
                        os.makedirs(out_path)
            
                    # Define file path to where tif is
                    if 'ghs_pop' == gpd:
                        # Define input raster to clip
                        in_ras = dirs + '/GHS_POP_E2015_GLOBE_R2019A_4326_30ss_V1_0.tif'
                        # Clip global tif using country extent 
                        system('gdalwarp -te %s %s %s %s -overwrite -co COMPRESS=LZW --config GDAL_CACHEMAX 2048 -co NUM_THREADS=ALL_CPUS -multi -of GTiff %s %s' %(xmin, ymin, xmax, ymax, in_ras, out_ras))
            
                    elif 'gpw' == gpd:
                        # Define input raster to clip
                        in_ras = dirs + '/gpw_v4_population_density_adjusted_to_2015_unwpp_country_totals_rev11_2015_30_sec.tif'
                        # Clip global tif using country extent 
                        system('gdalwarp -te %s %s %s %s -overwrite -co COMPRESS=LZW --config GDAL_CACHEMAX 2048 -co NUM_THREADS=ALL_CPUS -multi -of GTiff %s %s' %(xmin, ymin, xmax, ymax, in_ras, out_ras))
                        