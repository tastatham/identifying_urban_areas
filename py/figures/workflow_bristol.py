import os, sys, math
from affine import Affine
import dask, dask.dataframe as dd
import pandas as pd, numpy as np
import rasterio as rio, geopandas, shapely
from rasterio.plot import show
from rasterio import mask
import matplotlib.pyplot as plt, seaborn as sns, contextily as ctx
#!pip install xarray rioxarray
import xarray as xr, rioxarray
sys.path.append(os.path.abspath('../')) # Lazy path find
from src.data.db_conn import * 
from sqlalchemy import create_engine

# Define constants
schema = 'urban_pop'
gpd = 'worldpop'
iso = 'GBR'
dens_threshold = 1000

# Define paths
path = os.path.join('..')
data_path = os.path.join(path, 'data')
fig_path  = os.path.join('..', 'writing', 'ceus', 'draft', 'figures')
dbscan_path = os.path.join(data_path, 'output', 'parquet', 'dbscan', gpd)
worldpop_path = os.path.join(data_path, 'worldpop', 'MOSAIC_ppp_prj_2015/ppp_prj_2015_')
ras_path = worldpop_path + iso + '.tif'

# Define psql connection config
config_path = os.path.join(path, 'src/config/')
section = 'postgresql'
config_file = 'database.ini'
password = 'postgres'
# Create engine
engine = create_engine("postgresql://postgres:%s@comparing_urban_db:5432/urban_pop" %(password))  


# Define table_name
table_name = '_'.join([iso, gpd, str(dens_threshold)]).lower()

# Define postgres queries
urban_grid_query = sql.SQL('select uid, geom from urban_pop.global_grid where gid_0 LIKE {}').format(
        sql.Literal(iso) )
                        
create_urban_areas_query = sql.SQL(' \
    DROP TABLE IF EXISTS urban_pop.urban_areas_subset; \
    CREATE TABLE IF NOT EXISTS urban_pop.urban_areas_subset(geom geometry(Polygon, 4326)); \
    \
    WITH \
    cluster as( \
    SELECT \
        ST_Buffer((ST_Dump(St_Union(St_Buffer(geom, 0.0000000001)))).geom, -0.0000000001) AS geom \
    FROM \
        urban_pop.gbr_worldpop_1000 \
    GROUP BY \
        urban_settlements \
    ), \
     \
    noholes as( \
    SELECT \
        (ST_Dump(ST_Collect(ST_MakePolygon(geom)))).geom AS geom \
    FROM ( \
        SELECT \
            ST_ExteriorRing((ST_Dump(geom)).geom) AS geom \
        FROM \
            cluster \
        ) s \
    GROUP BY geom \
     \
    ) \
     \
    INSERT INTO urban_pop.urban_areas_subset (geom) \
    SELECT geom FROM noholes \
    ')                            

select_urban_areas_query = sql.SQL('select geom from urban_pop.urban_areas_subset')

# bounds are based on manually drawn polygon in qgis, where coords are;
## coords = [[-2.6508274999999797, 51.594559249999904, -2.5806275000000283, 51.64915924999986, -2.315427500000212, 
##51.62695924999988, -2.284227500000233, 51.329959250000094, -2.41862750000014, 51.21055925000017, -2.5368275000000584, 
##51.1649592500002, -2.85242749999984, 51.28255925000012, -2.9004274999998065, 51.43915925000001, -2.788227499999884, 
##51.496759249999975, -2.6508274999999797, 51.594559249999904]] 
## coords extracted from:  
#gdf = gpd.read_file(path)
#g = json.loads(df.to_json())
#coords = np.array(g['features'][0]['geometry']['coordinates'])
#list(coords.flatten())
minx, miny, maxx, maxy = -3.0004274183130915, 51.06506513695422, -2.1842322597546278, 51.749154685931885
# geopandas.GeoDataFrame(geometry = bris_ext.buffer(0.1), crs = 4326)
minx2, miny2, maxx2, maxy2  = -2.9004274999998065, 51.1649592500002, -2.284227500000233, 51.64915924999986

# Define class for dealing with db connections
db_conn = postgres_conn(section = section, config_path = config_path, config_file = config_file, country_iso3 = None)

# Read dbscan data with urban clusters
ddf = dd.read_parquet(dbscan_path, engine = 'pyarrow')
# Filter by threshold and nation
ddf = ddf[(ddf['pop_threshold'] == dens_threshold) & (ddf['nation'] == iso )]
df = ddf.compute()

# Read grids
gdf = db_conn.psql_to_gdf(urban_grid_query)
# Merge dbscan data with grid data
gdf_mer = pd.merge(gdf, df, on = 'uid')

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
urban_areas_gbr = db_conn.psql_to_gdf(select_urban_areas_query)

# Filter out urban areas based on buffer bounds 
urban_areas_gbr_bris = urban_areas_gbr.cx[minx:maxx, miny:maxy]
# Filter by extent
gdf_bris = gdf.cx[minx:maxx, miny:maxy]
#gdf1 = geopandas.sjoin(gdf, bris_ext, how = 'inner', op = 'intersects')
# Filter by extent
gdf_mer_bris = gdf_mer.cx[minx:maxx, miny:maxy]
gdf_mer_bris['cen'] = gdf_mer_bris.centroid
gdf_mer_bris = gdf_mer_bris.set_geometry('cen')

# Read raster
xds = rioxarray.open_rasterio(ras_path, masked=False, parse_coordinates=True)
# Clip raster using bounds
clipped = xds.rio.clip_box(minx, miny, maxx, maxy)

# Define src.transform (from rio)
affine = Affine(0.000833333330025608, 0.0, -3.0012499727564026, 0.0, -0.0008333333300242503, 51.74958332751485)

# Define subplots
fig, axs = plt.subplots(nrows = 2,ncols = 2, figsize = (10, 12), facecolor = 'white')
# Define tight layout & adjust subplots
plt.tight_layout()
plt.subplots_adjust(left = None, bottom = None, right = None, top = None, wspace = 0, hspace = -0.45)

# Flatten axis
axs = axs.flatten()

show(np.log(clipped+1), ax = axs[0], cmap = 'Reds', title='', transform = affine)
gdf_bris.plot(ax = axs[1], edgecolor = 'red', facecolor = 'none')
gdf_mer_bris.plot(ax = axs[2], color = 'red', markersize = 1.75)
urban_areas_gbr_bris.plot(ax = axs[3], linewidth = 0.9, edgecolor = 'red', facecolor = 'none')
# Add basemap for each axis
[ctx.add_basemap(ax = axs[ax], crs = gdf_bris.crs.to_string(), alpha = 1, zoom = 11, source = ctx.providers.Stamen.TonerLite) for ax in range(len(axs))]
show(np.log(clipped+1), ax = axs[0], cmap = 'Reds', title = '', transform = affine)

# Define limits
[axs[ax].set_xlim(minx2, maxx2) for ax in range(len(axs)) ] 
[axs[ax].set_ylim(miny2, maxy2) for ax in range(len(axs)) ]
# Set axis off
[axs[ax].set_axis_off() for ax in range(len(axs))];

# Annotate each subplot
axes = fig.get_axes()
texts = ['A)', 'B)', 'C)', 'D)']
for a,l in zip(axes, texts):
    a.annotate(l, xy=(0, 1.05), xycoords='axes fraction', fontsize = 15, weight = 'medium')

plt.savefig(os.path.join(fig_path, 'workflow_bristol.png'), dpi = 300, bbox_inches = 'tight', transparent=False);