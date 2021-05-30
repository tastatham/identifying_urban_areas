import os, sys
import pandas as pd, numpy as np
import matplotlib as mpl, matplotlib.pyplot as plt, seaborn as sns

# Define paths
path        = os.path.join('..')
output_path = os.path.join(path, 'data', 'output')
fig_path    = os.path.join(path, 'writing', 'ceus', 'draft', 'figures')

# Define gpd ls
gpd_ls = ['GHS-POP', 'GPW', 'Worldpop']
# Define thresholds list
pop_thresholds_ls = [2500, 5000, 10000]

#df = pd.read_csv(os.path.join(sett_count_path, 'national_urban_sett_counts.csv') )
df_thresholds = pd.read_csv(os.path.join(output_path, 'national_urban_sett_counts_pop_thresholds.csv') )
# Sort values
df_thresholds = df_thresholds.sort_values(['gpd', 'threshold', 'pop_threshold'], ascending=[True, True, True])
# Combine cols
df_thresholds['gpd_pop_threshold'] = df_thresholds['gpd'] + df_thresholds['pop_threshold'].astype(str)

# Drop old index col
df_thresholds = df_thresholds.drop(['Unnamed: 0'], axis=1)

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
    'USA': 'United States',
    'ZAF': 'South Africa'
}   

# Create new income group col based on dictionary
df_thresholds['iso_names'] = df_thresholds['nation'].map(iso_dic)

# Sort by nation
df_thresholds = df_thresholds.sort_values(['iso_names', 'gpd', 'threshold'], ascending=[True, True, True]).reset_index(drop=True)
# Define labs for legend
labs = pop_thresholds_ls * 3
# Define line style
style = ['-', '--', ':'] * len(gpd_ls)  
# Define colours - based on sns.husl_palette(3, h=.5) but with minor tweaks
hex_cols = [
    '#35ada1', '#38a9c6', '#7e97f4',
    #'#58AB56', '#35ada1', '#78A66C',
    '#df6df4', '#f66ab7', '#f77557',
    #'#AE6DF4', '#df6df4', '#95A1F4',
    '#D58C33', '#c09632', '#F0BA3E'
]
# Define pallette based on hex cols
pal = sns.color_palette(hex_cols)

# Define matplotlib params
params = {'figure.facecolor'      : 'white',
          'font.size'             :  24, 
          'figure.figsize'        : (8.25, 12),
          'figure.titleweight'    : 'bold',
          'figure.titlesize'      : 'xx-large',
          'axes.labelsize'        : 'large',
          'axes.titlesize'        : 'x-large',  
          'xtick.labelsize'       : 'medium',
          'ytick.labelsize'       : 'large', 
          'legend.fontsize'       : 'large', 
          'legend.title_fontsize' : 'x-large',
          'legend.handlelength'   :  1.5, 
          'legend.labelspacing'   :  0.25}
plt.rcParams.update(params)

# Define relplot
g = sns.relplot(data = df_thresholds, 
                x = 'threshold', y = 'urban_settlements', 
                hue = 'gpd_pop_threshold', style = 'pop_threshold', col = 'iso_names', 
                col_wrap = 3, alpha = 1, palette = pal, ci = None, facet_kws={'sharey': False, 'sharex': True},
                height = 7.5, kind = 'line', linewidth = 2.5, legend = False)

# Set as col name
g.set_titles('{col_name}', fontweight = 'medium')

# Set y axis
g.set(ylabel = '') # Set all y labels
[g.axes[ax].set_ylabel('Urban settlement counts') for ax in [0, 3, 6, 9]]
# Set x axis
[g.axes[ax].set_xlabel('', fontsize = 32) for ax in [5, 6, 7, 8, 9] ] 
[g.axes[ax].set_xlabel('Population density thresholds', fontsize = 32) for ax in [8, 9, 10, 11]]; # Manually set centre label

# Flatten axis
axs = g.axes.flatten()

# Place legend at bottom of subplots
legend = plt.legend(ncol = 3, fancybox = True, shadow = True, title = 'Population count thresholds', labels = labs, 
           loc = 'upper center', bbox_to_anchor = (-0.75, -0.15));
# Seperate legend title to add text
legend._legend_box.sep = 25
# Add text to legend
plt.text(-5950, -1525,  'GHS-POP',  fontsize = 28, weight = 'medium', zorder = 10)
plt.text(-3950, -1525,  'GPW',      fontsize = 28, weight = 'medium', zorder = 10)
plt.text(-1900, -1525,  'Worldpop', fontsize = 28, weight = 'medium', zorder = 10);

plt.savefig(os.path.join(fig_path, 'national_urban_area_counts_portrait.png'), dpi = 300, bbox_inches = 'tight');