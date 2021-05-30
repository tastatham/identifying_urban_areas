import sys, os, numpy as np
sys.path.append(os.path.abspath('../')) # Lazy path find
from src.indicators.indicators import *

# Define config info for postgres
section = 'postgresql'
config_path = '../src/config/'
config_file = 'database.ini'

# Change iso list
iso_ls = sorted(['USA', 'GBR', 'IND', 'ARG', 'MEX', 'HTI', 'NGA', 'RWA', 'ZAF', 'KEN', 'IDN', 'THA'])
# List of gridded population datasets
gpd_ls = ['gpw', 'ghs_pop', 'worldpop']
# Pop density & count thresholds
dens_thresholds_ls = np.arange(200, 4100, 200).tolist()
pop_thresholds_ls = [2500, 5000, 10000]
# Define min urban density - used to filter out cells first before loading into memory
threshold = dens_thresholds_ls[0]

# Calcualte national urban counts
nuc = comparing_urban_indicators(section, config_path, config_file, iso_ls, gpd_ls, dens_thresholds_ls)
        
if __name__ == '__main__':
    
    # Create spatial index
    nuc.create_global_sid()
    # Create new postgres table containing xy coords for each grid
    nuc.create_global_xy_table()
    # xy postgres table to parquet
    nuc.psql_centroids_to_csv() # Change chunksize to larger value if running into memory problems
     
    # Compute dbscan and return intermediate results to parquet
    [nuc.comparing_urban_dbscan(threshold, gpd) for gpd in gpd_ls]
    # Calculate urban settlement counts for no pop thresholds and with pop thresholds
    nuc.comparing_urban_settlement_count(pop_thresholds_ls = None)
    nuc.comparing_urban_settlement_count(pop_thresholds_ls)
