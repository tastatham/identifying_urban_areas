#!/bin/bash

echo "Create study area figure"
python ./figures/study_areas.py
echo "Create workflow bristol figure"
python ./figures/workflow_bristol.py
echo "Create density thresholds figure"
python ./figures/density_thresholds_haiti.py
echo "Create national urban shares figure"
python ./figures/national_urban_shares.py
echo "Create national urban area counts figure"
python ./figures/national_urban_area_counts.py
echo "Create national urban area counts-income figure"
python ./figures/national_urban_area_counts_income.py

