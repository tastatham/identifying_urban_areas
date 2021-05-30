import os, itertools
import pandas as pd, numpy as np
import matplotlib.pyplot as plt, seaborn as sns, palettable as pltt

# Define paths
path      = os.path.join('..')
nus_path  = os.path.join(path,  'data', 'output', 'national_urban_shares.csv')
fig_path  = os.path.join(path, 'writing', 'ceus', 'draft', 'figures')
gadm_path = os.path.join(path, 'data', 'gadm', 'gadm36_levels.gpkg')

# Define lists
iso_ls = sorted(['GBR', 'USA', 'IND', 'ARG', 'MEX', 'HTI', 'NGA', 'RWA', 'ZAF', 'KEN', 'IDN', 'THA'])
# List of gridded population datasets
gpd_ls = ['ghs_pop', 'gpw', 'worldpop']
# Pop density thresholds
dens_thresholds_ls = np.arange(200, 4200, 200).tolist()
# Pop count thresholds
pop_thresholds_ls = [2000, 5000, 10000]

# Read DataFrame
df = pd.read_csv(nus_path)

# Drop old index col
df = df.drop(['Unnamed: 0'], axis=1)

# Create iso dic for explicit matching
iso_dic = {
    'ARG': 'Argentina',
    'GBR': 'United Kingdom', 
    'HTI': 'Haiti',
    'IDN': 'Indonesia',
    'IND': 'India',
    'KEN': 'Kenya',
    'MEX': 'Mexico',
    'NGA': 'Nigeria',
    'RWA': 'Rwanda', 
    'THA': 'Thailand',
    'USA': 'United States of America',
    'ZAF': 'South Africa'
}   

# Create new income group col based on dictionary
df['iso_names'] = df['nation'].map(iso_dic)
# Replace long USA to shortened version
df = df.replace(to_replace = 'United States of America', value = 'United States', regex = True)
# Sort values
df = df.sort_values(['iso_names', 'gpd', 'dens_threshold'], ascending = [True, True, True]) \
  .reset_index(drop = True)

# Path to wup
un_path = 'https://population.un.org/wup/Download/Files/WUP2018-F01-Total_Urban_Rural.xls'
# Read un wup 2018 total urban
df_un = pd.read_excel(un_path, sheet_name = 0, header = 16)
# Calculate urban share
df_un['un_urban_share'] = df_un['Percentage urban'] / 100

# Rename cols
df_un = df_un[['Region, subregion, country or area', 'un_urban_share']] \
    .rename(columns = {'Region, subregion, country or area': 'iso_names'})

# Create empty 
df_shares = pd.DataFrame()
df_shares['nation'] = list(df['nation'].unique()) 

# Create new income group col based on dictionary
df_shares['iso_names'] = df_shares['nation'].map(iso_dic)

# Create degurba percentages for urban centres and clusters
degurba_dic = {
    'ARG': [64, 26],
    'GBR': [58, 26], 
    'HTI': [49, 32],
    'IDN': [57, 24],
    'IND': [54, 24],
    'KEN': [20, 31],
    'MEX': [57, 21],
    'NGA': [54, 24] ,
    'RWA': [15, 54], 
    'THA': [31, 28],
    'USA': [48, 24],
    'ZAF': [46, 23]
}   
# Map urban centres & clusters to DataFrame
df_shares['share'] = df_shares['nation'].map(degurba_dic)
# Split urban centres and clusters as new cols 
df_shares[['urban_centre_per', 'urban_cluster_per']] = pd.DataFrame(df_shares.share.tolist(), index = df_shares.index)
# Drop column
df_shares = df_shares.drop('share', axis = 1)
# Calculate eu urban percentage and share
df_shares['eu_urban_per'] = df_shares['urban_centre_per'] + df_shares['urban_cluster_per']
df_shares['eu_urban_share'] = df_shares['eu_urban_per'] / 100
# Merge cols
df_shares = pd.merge(df_shares, df_un, on = 'iso_names')

# Define colour palette
pal = sns.husl_palette(3, h=.5)

# Define seaborn style
sns.set(style = 'ticks', font_scale = 2)

# Define matplotlib params
params = {'figure.facecolor'      : 'white',
          'font.size'             :  24, 
          'figure.figsize'        : (8.25, 12),
          'figure.titleweight'    : 'bold',
          'figure.titlesize'      : 'xx-large',
          'axes.labelsize'        : 'large',
          'axes.titlesize'        : 'x-large',  
          'xtick.labelsize'       : 'large',
          'ytick.labelsize'       : 'large', 
          'legend.fontsize'       : 'large', 
          'legend.title_fontsize' : 'x-large',
          'legend.handlelength'   :  1.5, 
          'legend.labelspacing'   :  0.25}
plt.rcParams.update(params)

# Define x tick labs
legend_labs = ['GHS-POP', 'GPW', 'WorldPop']

# Define facetgrid
g = sns.relplot(x = 'dens_threshold', y = 'urban_prop', hue = 'gpd', col = 'iso_names', 
                palette = sns.color_palette(pal), 
                data = df, 
                col_wrap = 3, height = 7.5, 
                kind = 'line', linewidth = 2.5, 
                ci = None, legend = False)
# Flatten axis
axs = g.axes.flat

# Add lines
for ax in range(len(axs)):
    axs[ax].axhline(y = df_shares['eu_urban_share'][ax], linestyle = '--', color = 'black')
    axs[ax].axhline(y = df_shares['un_urban_share'][ax], linestyle = ':', color = 'grey')
# Add labels
for ax in [1, 2, 3, 4, 6, 7, 9, 11]:
    axs[ax].text(4000, df_shares['eu_urban_share'][ax] + 0.02, 'EU', fontsize = 20, fontweight = 'bold')
    axs[ax].text(4000, df_shares['un_urban_share'][ax] + 0.02, 'UN', fontsize = 20, fontweight = 'bold')
# Manual specify certain labels to avoid overlaps
axs[0].text(4000, df_shares['eu_urban_share'][0] - 0.05, 'EU', fontsize = 20, fontweight = 'bold')
axs[0].text(4000, df_shares['un_urban_share'][0] + 0.02, 'UN', fontsize = 20, fontweight = 'bold')

axs[5].text(4000, df_shares['eu_urban_share'][5] - 0.05, 'EU', fontsize = 20, fontweight = 'bold')
axs[5].text(4000, df_shares['un_urban_share'][5] + 0.02, 'UN', fontsize = 20, fontweight = 'bold')

axs[8].text(4000, df_shares['eu_urban_share'][8] + 0.02, 'EU', fontsize = 20, fontweight = 'bold')
axs[8].text(4000, df_shares['un_urban_share'][8] - 0.05, 'UN', fontsize = 20, fontweight = 'bold')

axs[10].text(4000, df_shares['eu_urban_share'][10] + 0.02, 'EU', fontsize = 20, fontweight = 'bold')
axs[10].text(4000, df_shares['un_urban_share'][10] - 0.05, 'UN', fontsize = 20, fontweight = 'bold')

[g.axes[ax].set_xlabel('') for ax in [5, 6, 7, 8, 9]] 
#g.set(ylabel = 'Urban share') # Set all y labels
[g.axes[ax].set_xlabel('Population density thresholds', fontsize = 32) for ax in [9, 10, 11]] # Manually set centre label
[g.axes[ax].set_ylabel('Urban share', fontsize = 32) for ax in [0, 3, 6, 9]]
g.set_titles("{col_name}", fontweight = 'medium')
# Adjust subplots for readability 
g.fig.subplots_adjust(top=0.9)

# Place legend at bottom of subplots
plt.legend(ncol = 3, fancybox = True, shadow = True, title = 'Gridded population datasets', labels = legend_labs, 
           loc = 'upper center', bbox_to_anchor = (-0.55, -0.15))
# Save image
plt.savefig(fig_path + '/' + 'national_urban_shares_portrait.png', dpi = 300, bbox_inches = 'tight');