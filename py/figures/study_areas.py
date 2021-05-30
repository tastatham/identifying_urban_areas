import os
import pandas as pd, numpy as np
import geopandas as gpd, contextily as ctx
import matplotlib.pyplot as plt, matplotlib.patches as mpatches, seaborn as sns

# Define path
path = os.path.join('..')
fig_path  = os.path.join(path, 'writing', 'ceus', 'draft', 'figures')
gadm_path = os.path.join(path, 'data', 'gadm', 'gadm36_levels.gpkg')

# Define countries
iso_ls = ['GBR', 'USA', 'IND', 'ARG', 'MEX', 'HTI', 'NGA', 'RWA', 'ZAF', 'KEN', 'IDN', 'THA']

# Read gadm level 1
world = gpd.read_file((gadm_path), layer = 'level0', crs = '4326')
# Subset countries in iso_ls
countries = world[world['GID_0'].isin(iso_ls)]

# Define legend font size
plt.rcParams["legend.fontsize"] = 10

# Define subplots 
fig, ax = plt.subplots(figsize = (8.25, 12), facecolor = 'white')

# Add case study countries to plot
countries.plot(ax = ax, facecolor = 'red', edgecolor = 'none', linewidth = 0.1, lw = 0.5, label = 'Nations', alpha = 0.4)
countries.plot(ax = ax, facecolor = 'none', edgecolor = 'red', linewidth = 0.7, lw = 0.5, label = 'Nations', alpha = 1)
# Set axis 
ax.set_xlim(-180, 180)
ax.set_ylim(-90, 90)
# Remove axis 
ax.set_axis_off()
# Add basemap
ctx.add_basemap(ax, source = ctx.providers.Stamen.TonerLite, crs = countries.crs.to_string(), alpha = 0.75)

# Define manual legend
lab = mpatches.Patch(edgecolor = 'red', alpha = 0.4, facecolor = 'red', linewidth = 0.7, label = 'Study areas')
plt.legend(handles = [lab], 
           ncol = 2, loc = 'upper right', fancybox = True);

plt.savefig(os.path.join(fig_path, 'study_areas.png'), dpi = 300,  bbox_inches = 'tight');