import sys, os, numpy as np
sys.path.append(os.path.abspath('../')) # Lazy path find
from src.indicators.indicators import *

# Define config info for postgres
section = 'postgresql'
config_path = '../src/config/'
config_file = 'database.ini'

# Change iso list
#iso_ls = sorted(['USA', 'GBR', 'IND', 'ARG', 'MEX', 'HTI', 'NGA', 'RWA', 'ZAF', 'KEN', 'IDN', 'THA'])
iso_ls = sorted(['RWA', 'HTI'])

# List of gridded population datasets
gpd_ls = ['gpw', 'ghs_pop', 'worldpop']
# Pop density thresholds
dens_thresholds_ls = np.arange(200, 4100, 200).tolist()

# Define class for calculating national urban shares
nus = comparing_urban_indicators(section, config_path, config_file, iso_ls, gpd_ls, dens_thresholds_ls)

if __name__ == '__main__':

    # Groups all zonal output csv files by each gpd and exports as parquet to "/data/zonal/parquet/%s" where %s is gpd
    nus.comparing_urban_csv_to_parquet()

    # Reads parquet files in /data/zonal/parquet/%s and computes national urban shares
    nus.compute_national_urban_shares()
