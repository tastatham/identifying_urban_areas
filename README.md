# Identifying urban areas
This repository hosts all of the code (both analysis and figures) used in the "identifying urban areas: A new approach and comparison of national urban metrics with gridded population data"

## Setting up computational environment & running analysis
We initially setup the analysis to use pure Python but we quickly realised that current packages are limited when conducting international scale geospatial analysis.
To overcome these limitations, we combined multiple geospatial tools, including geospatial packages in Python (geopandas, rasterio, xarray etc.) with GDAL, postgreSQL with postGIS & raster extensions and Exactextract. 
We used Docker to overcome dependency issues when setting up the computational environment and we provide a docker-compose.yml file.
You can of course opt to setup the computational environment yourself & update the scripts where database connections are defined.
The yml file defines two docker builds that are linked together using a network bridge.

The first contains:

    - Jupyter notebook server
    - Exactextract

The second contains:

    - PostgreSQL
    - with postGIS & raster extension

You must define the  `gid`, `uid` and `ports` in the docker-compose.yml (based on your machine).

## Repository breakdown 

### /py 
Directory contains all of the scripts used to reproduce the analysis and figures.
The most important aspect is to ensure all of the data has downloaded correctly in data_download.py. If not, then the analysis/figures will fail. 
For convenience, we have provided two bash scripts that run the analysis and figures: 

    - analysis.sh
        * data_download.py
        * create_vector_grid.py
        * zonalstats.py
        * compute_national_urban_counts.py
        * compute_national_urban_shares.py

    - figures.sh
        * study_areas.py
        * workflow_bristol.py
        * density_thresholds_haiti.py
        * national_urban_shares.py
        * national_urban_areas_counts.py
        * national_urban-areas_counts_income.py

### Download data used to create identified urban areas
    - /src/data
    - download_data.py - Downloads & crops data 
    - db_conn.py - Defines connections to postgres database for running specific queries & submitting data to and from postgres 
    - Zonalstats.py - calculates zonal statistics
    
### Calculate urban indicators
    - /src/indicators

### PostgreSQL functions
    - /src/sql
    
### Database configuration 
    - /src/config
    - database.ini - defines db params
    - config.py - adds db params as env variables

### Optional 
    - /src/build
    - Helps us check database setup
