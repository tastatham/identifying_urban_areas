import os, sys
import pandas as pd
sys.path.append(os.path.abspath('../'))
from src.config.config import *
from src.data.download_data import * 

path = os.path.join('..')
data_path = os.path.join(path, 'data')
gadm_path = os.path.join(data_path, 'gadm', 'gadm36_levels.gpkg')

# Define schema to create
schema = 'urban_pop'

# Change iso list
iso_ls = sorted(['USA', 'GBR', 'IND', 'ARG', 'MEX', 'HTI', 'NGA', 'RWA', 'ZAF', 'KEN', 'IDN', 'THA'])
# Define class for dealing with db connections
db_conn = postgres_conn(section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini', country_iso3 = iso_ls)

# Create / drop postgres schema
try:
    db_conn.drop_schema(schema) # If necessary
except:
    pass
db_conn.create_schema(schema)

dd = data_download(section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini', country_iso3 = iso_ls)

# Download gadm boundaries
#dd.gadm_boundaries()

# Upload nation boundaries (level0) to postgres
db_conn.gpkg_to_psql(gadm_path, schema, table = 'gadm_level0', s_src = 4326, level = 'level0')

# Upload nation boundaries (level1) to postgres
db_conn.gpkg_to_psql(gadm_path, schema, table = 'gadm_level1', s_src = 4326, level = 'level1')

# Download gpd
#dd.gpw()
#dd.ghs_pop()
#dd.worldpop()

# (global) GPW raster that will serve as our vector grid - after processing 
gpw_path   = '../data/gpw/gpw_v4_population_density_adjusted_to_2015_unwpp_country_totals_rev11_2015_30_sec.tif'
gpw_table  = 'gpw_global_2015_30_sec'
# Upload gpw postgres for creating vector grid
db_conn.raster_to_psql(gpw_path, schema, gpw_table, s_src = 4326)


