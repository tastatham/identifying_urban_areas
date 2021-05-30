import os, sys
import pandas as pd, numpy as np
import matplotlib.pyplot as plt, seaborn as sns


# Define paths
path        = os.path.join('..')
output_path = os.path.join(path, 'data', 'output')
fig_path    = os.path.join(path, 'writing', 'ceus', 'draft', 'figures')

# Read csv
df = pd.read_csv(os.path.join(output_path, 'national_urban_sett_counts_pop_thresholds.csv') )

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
df = df.sort_values(['iso_names', 'gpd', 'threshold'], ascending = [True, True, True]).reset_index(drop = True)

# Define dictionary to map country to income level 
income_dic = {
    'Rwanda'         : 'Low Income',
    'Haiti'          : 'Low Income',
    'India'          : 'Lower-Middle Income',
    'Kenya'          : 'Lower-Middle Income',
    'Nigeria'        : 'Lower-Middle Income',
    'South Africa'   : 'Upper-Middle Income',
    'Argentina'      : 'Upper-Middle Income',
    'Mexico'         : 'Upper-Middle Income',
    'Indonesia'      : 'Upper-Middle Income',
    'Thailand'       : 'Upper-Middle Income',
    'United Kingdom' : 'High Income', 
    'United States'  : 'High Income'
    }

# Create new income group col based on dictionary
df['income_group'] = df['iso_names'].map(income_dic)

# Prettify gpd names for plot  
df['gpd'] = df['gpd'].replace(['ghs_pop','gpw', 'worldpop'],['GHS-POP', 'GPW', 'WorldPop'])

# Drop old index col
df = df.drop(['Unnamed: 0', 'nation'], axis = 1)
# Log values to compare relative differences - absolute differences are too big
df['urban_settlements_log'] = np.log(df['urban_settlements'])

# Define sorter
sorter = ['Low Income', 'Lower-Middle Income', 'Upper-Middle Income', 'High Income']
# Create dic to define ordering that defines the order for sorting
sorterIndex = dict(zip(sorter, range(len(sorter))))
df['order'] = df['income_group'].map(sorterIndex)
# Sort values
df = df.sort_values(['order', 'gpd', 'pop_threshold'], ascending = [True, True, True]).reset_index(drop = True)

# Define palette
pal = sns.husl_palette(3, h = .5)

# Define matplotlib params
params = {'figure.facecolor'      : 'white',
          'font.size'             :  16, 
          'font.sans-serif'       : 'Arial',
          'figure.figsize'        : (10, 10),
          'figure.titleweight'    : 'bold',
          'figure.titlesize'      : 'x-large',
          'axes.labelsize'        : 'x-large',
          'axes.titlesize'        : 'x-large',  
          'xtick.labelsize'       : 'large',
          'ytick.labelsize'       : 'large', 
          'legend.fontsize'       : 'large', 
          'legend.title_fontsize' : 'x-large',
          'legend.handlelength'   :  1.5, 
          'legend.labelspacing'   :  0.25}

# Apply matplotlib params
plt.rcParams.update(params)

# Define catplot
g = sns.catplot(x = 'pop_threshold', y = 'urban_settlements_log',
                hue='gpd', col='income_group', col_wrap = 2,
                data = df, kind = 'violin', palette = pal, 
                height = 7.5, aspect = 1, legend = False);

# Set as col name
g.set_titles('{col_name}', fontweight = 'medium');

# Define xtick labels
xtick_labs = ['GHS-POP', 'GPW', 'Worldpop']

# Set x, y axis titles
#g.set(xlabel = 'Population thresholds')
[g.axes[ax].set_xlabel('Population thresholds') for ax in [2, 3]];

[g.axes[ax].set_ylabel('Urban settlement counts (log)') for ax in [0, 2]]; # Manually set centre label

# Place legend at bottom of subplots
plt.legend(ncol = 3, fancybox = True, shadow = True, title = 'Gridded population datasets', 
           loc = 'upper center', bbox_to_anchor = (0, -0.15));

plt.savefig(os.path.join(fig_path, 'national_urban_area_counts_income.png'), dpi = 300, bbox_inches = 'tight');