import os, sys
import pandas as pd
sys.path.append(os.path.abspath('../'))
from src.config.config import *
from src.data.db_conn import * 
from src.data.download_data import * 

schema = 'urban_pop'
# Define lists
#iso_ls = sorted(['GBR', 'USA', 'IND', 'ARG', 'MEX', 'HTI', 'NGA', 'RWA', 'ZAF', 'KEN', 'IDN', 'THA'])
iso_ls = sorted(['HTI', 'RWA'])
# Define pg conn
db_conn = postgres_conn(section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini', country_iso3 = iso_ls)
# Add user defined functions
db_conn.add_postgis_create_grid_functions()

# This loops through each stored iso alpha-3 code and creates stored vector geometries in the postgres server 
db_conn.create_grid(schema, table = 'global_grid')

# Define data download/crop
dd = data_download(section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini', country_iso3 = iso_ls)
# Crop gpd using nation bounds from gadm level 0
dd.crop_gpd(gpd = 'gpw', schema = schema, table = 'global_grid')
dd.crop_gpd(gpd = 'ghs_pop', schema = schema, table = 'global_grid')
