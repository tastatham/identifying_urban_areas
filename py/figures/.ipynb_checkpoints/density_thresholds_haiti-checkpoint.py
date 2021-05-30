import os, sys
import dask, dask.dataframe as dd
import pandas as pd, numpy as np
import geopandas as gpd, shapely
import matplotlib.pyplot as plt, seaborn as sns, contextily as ctx
import matplotlib.patches as mpatches
from sqlalchemy import create_engine
sys.path.append(os.path.abspath('../')) # Lazy path find
from src.data.db_conn import * 

# Define constants
schema = 'urban_pop'
gpd = 'worldpop'
iso = 'HTI'
dens_thresholds_ls = [200, 400, 800, 1600] 

# Define paths
path = os.path.join('..')
dbscan_path = os.path.join(path, 'data', 'output', 'parquet', 'dbscan', gpd)
fig_path  = os.path.join(path, 'writing', 'ceus', 'draft', 'figures')

config_path = os.path.join(path, 'src/config/')
section = 'postgresql'
config_file = 'database.ini'
password = 'postgres'

# Define table name to create
table_name = '_'.join([iso, gpd, 'explain_method'] ).lower()


# Define postgres queries
urban_grid_query = sql.SQL('select uid, geom from urban_pop.global_grid where gid_0 LIKE {}').format(
        sql.Literal(iso) )
                        
create_urban_areas_query = sql.SQL(' \
    DROP TABLE IF EXISTS urban_pop.hti_urban_areas_explain_method; \
    CREATE TABLE IF NOT EXISTS urban_pop.hti_urban_areas_explain_method(pop_threshold NUMERIC, geom geometry(Polygon, 4326)); \
    \
    WITH \
    cluster as( \
    SELECT \
        pop_threshold AS pop_threshold, ST_Buffer((ST_Dump(St_Union(St_Buffer(geom, 0.0000000001)))).geom, -0.0000000001) AS geom \
    FROM \
        urban_pop.hti_worldpop_explain_method \
    GROUP BY \
        pop_threshold, urban_settlements \
    ), \
     \
    noholes as( \
    SELECT \
        pop_threshold, (ST_Dump(ST_Collect(ST_MakePolygon(geom)))).geom AS geom \
    FROM ( \
        SELECT \
            pop_threshold, ST_ExteriorRing((ST_Dump(geom)).geom) AS geom \
        FROM \
            cluster \
        ) s \
    GROUP BY pop_threshold, geom \
     \
    ) \
     \
    INSERT INTO urban_pop.hti_urban_areas_explain_method (geom, pop_threshold) \
    SELECT geom, pop_threshold FROM noholes \
    ')                            

select_urban_areas_query = sql.SQL('select * from urban_pop.hti_urban_areas_explain_method')

# Read dbscan data with urban clusters
ddf = dd.read_parquet(dbscan_path, engine = 'pyarrow')

# Filter by threshold and nation
ddf = ddf[(ddf['pop_threshold'].isin(dens_thresholds_ls )) & (ddf['nation'] == iso )]
df = ddf.compute()

# Define class for dealing with db connections
db_conn = postgres_conn(section=section, config_path=config_path, config_file=config_file, country_iso3=None)

# Read grids
gdf = db_conn.psql_to_gdf(urban_grid_query)
# Merge dbscan data with grid data
gdf_mer = pd.merge(gdf, df, on = 'uid')

# Create engine
#password = 'mypassword'
engine = create_engine("postgresql://postgres:%s@comparing_urban_db:5432/urban_pop" %(password))  

#engine = create_engine('postgresql://postgres:postgres@comparing_urban_db:5432/comparing_urban')
# gdf to postgis
gdf_mer.to_postgis(name = table_name, con = engine, schema = schema, if_exists = 'replace')  

# Create urban areas                        
try:
    conn = None  
    # Define db_params
    dbParams = config(section, config_path, config_file)
    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    # Open connection
    conn = psql.connect(**dbParams)
    curr = conn.cursor()
    # Execute query
    curr.execute(create_urban_areas_query)         
    # Commit query to database
    conn.commit()
    # Print functions successfully added to postgreSQL
    print('Successfully executed postgreSQL query...')
    
# Return exception if process is unsuccessful 
except (Exception, psql.DatabaseError) as error :
    print ("Error while running query", error)
finally:
    # Close all database connections
        if(conn) is not None:
            curr.close()
            conn.close()
            print("PostgreSQL connection is closed")

# Select urban areas
urban_areas_explain = db_conn.psql_to_gdf(select_urban_areas_query)

fig, axs = plt.subplots(nrows = 2,ncols = 2, figsize = (15, 15))
plt.tight_layout()
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0, hspace = -0.35)
axs = axs.flatten()
fig.patch.set_facecolor('white')

for ax in range(len(dens_thresholds_ls)):
        urban_areas_explain[urban_areas_explain['pop_threshold'] == dens_thresholds_ls[ax]].plot(facecolor='red', edgecolor='none', linewidth=0.1, alpha = 0.4, ax=axs[ax])
        urban_areas_explain[urban_areas_explain['pop_threshold'] == dens_thresholds_ls[ax]].plot(facecolor='none', edgecolor='red', linewidth=0.7, ax=axs[ax])
        axs[ax].set_axis_off()
        ctx.add_basemap(axs[ax],  source = ctx.providers.Stamen.TonerLite, crs = 4326, alpha = 0.7)

axes = fig.get_axes()
texts = ['A)', 'B)', 'C)', 'D)']
for a,l in zip(axes, texts):
    a.annotate(l, xy = (0, 1.05), xycoords = 'axes fraction', fontsize = 15, weight = 'medium')

# Define manual legend
lab = mpatches.Patch(edgecolor = 'red', alpha = 0.4, facecolor = 'red', linewidth = 0.7, label = 'Urban areas')
plt.legend(handles=[lab], 
           ncol = 2, loc = 'upper right', fontsize = 15, fancybox = True);
    
plt.savefig(os.path.join(fig_path, 'density_thresholds_haiti.png'), dpi = 300, bbox_inches = 'tight');
