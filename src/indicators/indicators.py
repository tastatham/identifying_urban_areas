# Import libraries
import os, glob, time, sys
import pandas as pd, numpy as np
import psycopg2 as psql
from psycopg2 import sql
import dask
import dask.dataframe as dd
from dask.multiprocessing import get
from dask.distributed import Client, progress
from sklearn.cluster import DBSCAN

sys.path.append(os.path.abspath('../'))
from src.config.config import config # Take in a PostgreSQL table and outputs a pandas dataframe
from src.data.db_conn import postgres_conn # Take in a PostgreSQL table and outputs a pandas dataframe

class comparing_urban_indicators():    

    def __init__(self, section, config_path, config_file, iso_ls, gpd_ls, dens_thresholds_ls):
        
        """
        Parameters
        ----------
                
        """
        self.section             = section
        self.config_path         = config_path 
        self.config_file         = config_file
        self.iso_ls              = iso_ls
        self.gpd_ls              = gpd_ls 
        self.dens_thresholds_ls  = dens_thresholds_ls

##################
##  PROCESSING  ##
##################

    def dask_csv_to_parquet(self, gpd = 'gpw'):

        """
        Function to convert list of csvs into parquet, split by gpd using Dask
        """
        
        # General path to reuse
        path = os.path.join('..', 'data', 'zonal' )
        # Define list of full paths to csv for gpd 
        zonal_csv_path = sorted(glob.glob(os.path.join(path, '*%s*' % gpd) ))
        # Define path to output parquet file to 
        parquet_path = os.path.join(path, 'parquet', gpd) + '/'
        # Create path if not exists
        if not os.path.exists(parquet_path):    
            os.makedirs(parquet_path)
            
        # Define cols to import as
        cols = ['uid', 'pop_count']
        # Define types
        type_dict = {cols[0]: 'category', cols[1]: 'object'}

        # Read each csv in zonal_csv path 
        ddf = dd.read_csv(zonal_csv_path, names = cols, header=0, dtype = type_dict, na_values = ['-nan'])
        # Create new nation col based on first three characters in string
        ddf['nation'] = ddf['uid'].str[:3] 
        # Create new gpd col 
        ddf['gpd'] = gpd
        # Convert to numeric
        ddf['pop_count'] = ddf['pop_count'].astype(np.float32)
        # Make sure uid is an obj type
        ddf['uid'] = ddf['uid'].astype(object)
        # Export as parquet folder where n parquet partitions equals n csv
        ddf.to_parquet(parquet_path, engine = 'pyarrow', schema = 'infer')
        return()

    def comparing_urban_csv_to_parquet(self):
        
        """
        Function for concating all csvs to parquet by gpd using Dask
        """
        
        # This step should take the longest - converting the csv into parquet format
        [self.dask_csv_to_parquet(gpd) for gpd in self.gpd_ls]
        
        return()
    
    def read_zonal_parquet(self, gpd = 'gpw'):
        
        """
        Function to read parquet file using Dask
        """

        # Define path 
        path = os.path.join('..', 'data', 'zonal', 'parquet', gpd) 
        # Get parquet file
        ddf = dd.read_parquet(path, engine = 'pyarrow')

        return(ddf)
    
    def create_global_sid(self):
        
        """
        Create spatial index for global_grid table
        """
        
        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
        
        # Define queries
        create_sid_query = 'CREATE INDEX global_grid_sid ON urban_pop.global_grid USING GIST (geom)';
        create_cluster_query = 'CLUSTER urban_pop.global_grid USING global_grid_sid';
        
        try:
            # Set conn as None before trying connection
            conn = None
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
                
            with conn.cursor() as c:
                c.execute(create_sid_query)
                c.execute(create_cluster_query)

                # Commit query 
                conn.commit()
                # Print functions successfully added to postgreSQL
                print('Successfully created temporary subdivided gadm table')
              
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn):
                    c.close()
                    conn.close()
                    print("PostgreSQL connection is closed") 

    def create_global_xy_table(self):
        
        """
        Function to create a new table containing gid_0, uid x, y coords based on centroid of polygons from urban_pop.global_grid
        """
        
        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
        
        try:
            # Set conn as None before trying connection
            conn = None
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # Open connection
            conn = psql.connect(**dbParams)
                
            with conn.cursor() as c:
        
                # SELECT x,y, uid FROM global_grid
                create_cen_table = sql.SQL('DROP TABLE IF EXISTS urban_pop.global_cen; \
                    CREATE TABLE urban_pop.global_cen (fid SERIAL PRIMARY KEY, uid TEXT, gid_0 TEXT, x NUMERIC, y NUMERIC);')
            
                insert_cen_table = sql.SQL('WITH cen AS (SELECT uid, gid_0, st_x(st_centroid(geom)) as x, st_y(st_centroid(geom)) as y \
                    FROM urban_pop.global_grid WHERE gid_0 IN ({}) ) INSERT INTO urban_pop.global_cen (uid, gid_0, x, y) SELECT uid, gid_0, x, y FROM cen').format(
                sql.SQL(', ').join(map(sql.Literal, self.iso_ls) ) ) 
                c.execute(create_cen_table)
                c.execute(insert_cen_table)
                conn.commit()
                print('Successfully created global xy tables')
            
        # Return exception if process is unsuccessful 
        except (Exception, psql.DatabaseError) as error :
            print ("Error while running query", error)
        finally:
            # Close all database connections
                if(conn):
                    c.close()
                    conn.close()
                    print("PostgreSQL connection is closed")

    def psql_centroids_to_csv(self):
        
        """
        Function to export global_grid postgres table containing x,y values csv
        """
        
        # Set conn as None before trying connection
        conn = None

        # Define folder to create 
        path = os.path.join('..', 'data', 'output')
        
        # Create folder if does not already exist
        if not os.path.exists(path):    
            os.makedirs(path)
            
        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
        
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        # Open connection
        conn = psql.connect(**dbParams)
        
        global sql # lazy
        # Define sql query to get table
        sql = sql.SQL('select uid, x, y from urban_pop.global_cen where gid_0 in ({})').format(
        sql.SQL(', ').join(map(sql.Literal, self.iso_ls) ) )
        
        # For each defined chunk, read sql table using pandas and export to csv
        for chunk in pd.read_sql_query(sql, conn, chunksize=10000): # Change this if running into memory constraints
            # Select the ones you want
            chunk = chunk[['uid', 'x', 'y']]
            # append to chunk to csv
            chunk.to_csv(os.path.join(path, 'global_cen.csv'), mode = 'a', header = False, sep = ',', encoding = 'utf-8') # ignore header to prevent insertion et
        
        conn.close()
        
        return()

    def merge_global_cen_zonal(self, threshold = 200, gpd = 'gpw'):
        
        """
        Function to merge csv containing global centroids with zonal statistic parquet file 
        """

        # from merge global_cen_zonal
        global_cen_path = os.path.join('..', 'data', 'output', 'global_cen.csv')
        # Define col names
        cols = ['uid', 'x', 'y'] 
        # Define col types
        dtypes = {cols[0]: str, cols[1]: np.float32, cols[2]: np.float32}
        # Read global centroid csv file
        global_cen_df = pd.read_csv(global_cen_path, names = cols, dtype = dtypes, sep=',')
        # Read zonal parquet file
        gpd_ddf = self.read_zonal_parquet(gpd)
        # Filter zonal parquet
        gpd_ddf = gpd_ddf[gpd_ddf['pop_count'] > threshold ]
        # Get filtered pop counts
        gpd_df = gpd_ddf.compute()
        # Merge df and parquet
        df = pd.merge(gpd_df, global_cen_df, on = 'uid', how = 'inner')
        
        return(df)

    def pop_thresholds(self, df, pop_threshold, iso):
        
        """
        Function for subsetting aggregated data by population density thresholds 
        """
        
        if iso is None:
            # Coyp dataframe
            df = df.copy()
            # Subset by threshold and iso
            df = df[df['pop_count'] >= pop_threshold ] 
            # Assign col
            df['pop_threshold'] = pop_threshold
        elif iso is not None:
            # Coyp dataframe
            df = df.copy()
            # Subset by threshold and iso
            df = df[(df['pop_count'] >= pop_threshold) & (df['nation'] == iso )]
            # Assign col
            df['pop_threshold'] = pop_threshold
            
        return(df)
    
##################
##  INDICATORS  ##
##################
    
    def national_urban_share(self, gpd = 'gpw', threshold = 1000):
        
        """
        Function for calculating national urban shares
        """

        # Read parquet 
        ddf = self.read_zonal_parquet(gpd)
        # Drop col
        ddf = ddf.drop('uid', axis = 1)
        # Calculate urban pop sum for a single density threshold for each nation 
        urban_pop = ddf[ddf['pop_count'] >= threshold ].groupby('nation')[['pop_count']].sum()
        # Calculate total pop sum for each nation 
        total_pop = ddf.groupby('nation')[['pop_count']].sum()
        # Calculate urban_proportion
        df = urban_pop / total_pop
        # Define col names
        df.columns = ['urban_prop']
        # Create new col
        df['dens_threshold'] = threshold
        # Create new col
        df['gpd'] = gpd
        # Reset index
        df = df.reset_index(drop=False)

        return(df)
        
    def compute_national_urban_shares(self):
        
        """
        Function for calculating national shares for each gpd and density threshold defined
        """
        
        # Lazily evaluate computing national urban shares for each gpd and density threshold
        ddf = [self.national_urban_share(gpd, threshold) for threshold in self.dens_thresholds_ls for gpd in self.gpd_ls ]

        # Compute from lazy evaluation for  each gpd and density threshold 
        results = dask.compute(*ddf)
        # Concat list of results
        df = pd.concat(results)
        # Sort values
        df.sort_values(['nation', 'gpd', 'urban_prop'], inplace=True, ascending = [True, True, False])
        # Define output path
        path = os.path.join('..', 'data', 'output/') 
        if not os.path.exists(path) :
            os.mkdir(path)
        # Export to csv
        df.to_csv(os.path.join(path, 'national_urban_shares.csv') )  
        
        return(df)

    def dbscan(self, df, dist = 1000):
        
        """
        Function to run dbscan and returns dbscan cluster labels
        """
        
        # Calculate radians
        coords = np.radians(df[['x','y']].values)
       
        # Earth radius
        kms_per_radian = 6371.0088
        # Define epsilon 
        epsilon = (dist / 1000) / kms_per_radian
        # Define start time
        start_time = time.time()
        # Cluster grid centroids
        dbscan = DBSCAN(eps = epsilon, min_samples = 1, algorithm = 'ball_tree', metric = 'haversine').fit(coords)
        # Define labels
        cluster_labels = dbscan.labels_ 

        return(cluster_labels)

    def urban_dbscan(self, df, dist = 1000, iso = 'GBR', threshold = 1000):
        
        """
        Function for returning pandas DataFrame with urban cluster labels from dbscan for a single iso, density threshold and gpd.
        """
        
        df = self.pop_thresholds(df, threshold, iso) 

        # Get gpd
        gpd = list( df['gpd'].unique() ) 
        gpd = ' '.join([str(elem) for elem in gpd] )
        
        # Start time
        start_time = time.time()
        # Extract cluster labels from dbscan
        cluster_labels = self.dbscan(df)
        
        # Define n clusters
        num_clusters = len(set(cluster_labels)) 

        # Print message about clustered data 
        message = 'Clustered {:,} points down to {:,} clusters, for {:.2f}% compression in {:,.2f} seconds for nation: {:}, gpd: {:}, density threshold: {:}m'
        print(message.format(len(df), num_clusters,100*(1 - float(num_clusters) / len(df)), time.time()-start_time, iso, gpd, threshold))   

        # Drop x,y cols to save space
        df = df.drop(['x', 'y'], axis=1)
        
        # Set dbscan labels as "settlements"
        df['urban_settlements'] = cluster_labels
        df['threshold'] = threshold
        
        df['urban_settlements'] = df['urban_settlements'].astype('float32')
        df['threshold'] = df['threshold'].astype('float32')
        
        return(df)

        
    def comparing_urban_dbscan(self, threshold, gpd):
    
        """
        Function for exporting dbscan urban clusters to parquet for each nation and density thresholds
        """
        
        # Define output path for parquet file
        path = os.path.join('..', 'data', 'output', 'parquet', 'dbscan', gpd)
        
        # Set df as nothign
        df = None
    
        # Merge global centroid csv with zonal parquet
        df = self.merge_global_cen_zonal(threshold = 200, gpd = gpd)
        # Run dbscan for each nation and density threshold list 
        dbscan_ls = [self.urban_dbscan(df, dist = 1000, iso = iso, threshold = threshold) for iso in self.iso_ls for threshold in self.dens_thresholds_ls ]
        # Concat list of df to a single df
        dbscan_df = pd.concat(dbscan_ls)
        # Transform from pandas to dask 
        ddf = dd.from_pandas(dbscan_df, npartitions = 5) # change npartitions if necessary, where npartition = ~100mb
        # Export to parquet
        ddf.to_parquet(path, engine = 'pyarrow', schema = 'infer')
    
    def urban_settlement_count(self, gpd, pop_thresholds_ls):
        
        """
        Function for calculating n urban settlements for each input gpd for all settlement sizes OR
        a specified population threshold e.g. 2000, 5000 or 10000 population per settlement
        
        Returns Pandas DataFrame containing number of urban settlements per iso and density threshold

        """
        
        # Define path
        path = os.path.join('..', 'data', 'output', 'parquet', 'dbscan', gpd)
        
        # Read as df
        df = pd.read_parquet(path)

        if pop_thresholds_ls is None:
            
            # Define list of cols to aggregate by
            cols = ['nation', 'gpd', 'threshold', 'urban_settlements']
            # Get unique settlements for each nation, gpd and threshold
            urban_sett_count = df[cols].groupby(cols[:-1])[cols[-1]].nunique().reset_index(drop = False)
                        
        elif pop_thresholds_ls is not None:
            
            # Define list of cols to aggregate by
            cols1 = ['nation', 'gpd', 'threshold', 'urban_settlements', 'pop_count']
            cols2 = ['nation', 'gpd', 'threshold', 'pop_threshold', 'urban_settlements']

            # Aggregate all population grid counts for each settlement 
            df_agg = df[cols1].groupby(cols1[:-1]).sum().reset_index(drop = False)
            # Apply pop thresholds to each combination
            threshold_df_ls = [self.pop_thresholds(df_agg, pop_threshold, iso = None) for pop_threshold in pop_thresholds_ls ] 
            # Concat df
            df_agg2 = pd.concat(threshold_df_ls)
            # Get unique settlements for each nation, gpd and threshold
            urban_sett_count = df_agg2[cols2].groupby(cols2[:-1])[cols2[-1]].nunique().reset_index(drop = False)
        
        return(urban_sett_count)

    def comparing_urban_settlement_count(self, pop_thresholds_ls):
        
        """
        Calculate urban settlement counts for gridded population dataset
        """

        path = os.path.join('..', 'data', 'output')
        
        if pop_thresholds_ls is None:
            
            ls = [self.urban_settlement_count(gpd, pop_thresholds_ls) for gpd in self.gpd_ls ] 
        
            df = pd.concat(ls)
            
            name = 'national_urban_sett_counts.csv'
            df.to_csv( os.path.join(path, name) ) 
            
        elif pop_thresholds_ls is not None:
            
            ls = [self.urban_settlement_count(gpd, pop_thresholds_ls) for gpd in self.gpd_ls ] 
        
            df = pd.concat(ls)
            
            name = 'national_urban_sett_counts_pop_thresholds.csv'
            df.to_csv( os.path.join(path, name) )   
        
        return(df)