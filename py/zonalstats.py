import os, sys
import pandas as pd
sys.path.append(os.path.abspath('../'))
from src.config.config import *
from src.data.zonal_stats import * 

# Define paths
path = os.path.join('..')
data_path = os.path.join('..', 'data')
zonal_path = os.path.join(data_path, 'zonal')

# Define schema to create
schema = 'urban_pop'

# Change iso list
#iso_ls = sorted(['USA', 'GBR', 'IND', 'ARG', 'MEX', 'HTI', 'NGA', 'RWA', 'ZAF', 'KEN', 'IDN', 'THA'])
iso_ls = sorted(['HTI', 'RWA'])
# List of gridded population datasets
gpd_ls = ['gpw', 'ghs_pop', 'worldpop']


# Setup zonal class for applying zonal stats using Exactextract
zonal = zonal_stats(section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini', country_iso3 = iso_ls)

# Loop through each gpd and compute zonal stats
zonal.comparing_urban_zonal_stats(zonal_path = zonal_path, fid = 'uid', stats = 'sum', gpd_ls = gpd_ls, schema = schema, table = 'global_grid')
