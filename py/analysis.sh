#!/bin/bash

#echo "Downloading all datasets before running analysis."
python ./py/data_download.py
echo "Create vector grid using GPW and crop raster using nation extent."
python create_vector_grid.py
echo "Run zonal statistics using Exactextract"
python zonalstats.py
echo "Compute national urban shares"
python compute_national_urban_shares.py
echo "Compute national urban area counts"
python compute_national_urban_area_counts.py
