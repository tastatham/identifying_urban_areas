    """                
    def zonalstats_to_psql(self, zonal_path = '../data/zonal/', db = 'gpw', schema = 'urban_pop', table = 'zonal_stats'):
        
        dbParams = config(self.section, self.config_path, self.config_file)
        
        conn = None
        # Open connection
        conn = psql.connect(**dbParams)

        layer = '.'.join([schema, table])
        
        with conn.cursor() as c:
            sql_query = sql.SQL('CREATE TABLE IF NOT EXISTS {zonalstats}( ' \
                                'uid TEXT, gpw_sum TEXT, ghs_pop_sum TEXT, worldpop_sum TEXT, hrsl_sum TEXT);').format(
                                zonalstats = sql.SQL('.'.join([schema, table])) )
            c.execute(sql_query)
            # Commit query 
            conn.commit()
            # Print functions successfully added to postgreSQL
            print('Successfully created zonalstatistics table (if not exists)') 
        
        for iso in self.country_iso3:
            
            ras_path = zonal_path + iso + '_'+ db + '.csv'

            try:
                conn = None
                conn = psql.connect(**dbParams)

                with open(ras_path, 'r') as f:
                    cur = conn.cursor()
                    # Skip header row
                    next(f)
                    # Copy csv to table
                    cur.copy_from(f, layer, sep = ',', null = '-nan', columns = ('uid', db + '_sum') )
                    # Commit query 
                    conn.commit()
                    print('Successfully uploaded csv to postgres table...')
                    cur.close()
                    conn.close()
            # Return exception if process is unsuccessful 
            except (Exception, psql.DatabaseError) as error :
                print ("Error while running query", error)
            finally:
                # Close all database connections
                    if(conn) is not None:
                        cur.close()
                        conn.close()
                        print("PostgreSQL connection is closed")
    """

"""
    def calculate_nation_urban_shares(self, schema, gpd_ls = ['gpw', 'ghs_pop', 'worldpop'] , dens_thresholds_ls = list(np.arange(200, 4200, 200) ) ):
        
        """ Apply postgis functions to calcualte national urban shares based on each gpd and density threshold """
                
        # Connecting to db using params
        dbParams = config(self.section, self.config_path, self.config_file)
               
        for iso in self.country_iso3:    
            for gpd in gpd_ls:
                for threshold in dens_thresholds_ls:
                    
                    try:
                        # Set conn as None before trying connection
                        conn = None
                        # connect to the PostgreSQL server
                        print('Connecting to the PostgreSQL database...')
                        # Open connection
                        conn = psql.connect(**dbParams)
                            
                        with conn.cursor() as c:

                            # Call user generated function
                            c.callproc('create_national_urban_share', ( '.'.join([schema, 'zonal_stats']), '.'.join([schema, 'national_urban_share']),
                                                                      iso, gpd, threshold) )
                            # Commit query 
                            conn.commit()
                            # Print functions successfully added to postgreSQL
                            print('Successfully inserted into national urban share')
                          
                    # Return exception if process is unsuccessful 
                    except (Exception, psql.DatabaseError) as error :
                        print ("Error while running query", error)
                    finally:
                        # Close all database connections
                            if(conn):
                                c.close()
                                conn.close()
                                print("PostgreSQL connection is closed")                          
    """
    
class data_download:
    
    """
    Download gridded population data & gadm boundaries crop using nation extent if needed
    """
    
    def __init__(self, country_iso3 = None, country_name = None, continent_iso3 = None, continent_name = None):
        self.country_iso3     = country_iso3
        self.country_name     = country_name
        self.continent_iso3   = continent_iso3
        self.continent_name   = continent_name
        
    def crop_gpd(self, db):
        
            """
            Apply a function that crops gridded population datasets by extent of country.
            The preferred way is to present the country in ISO standard to avoid spelling errors but 
            country names in English can also be presented.
            This should only be applied to the GPW and GHS-POP
            
            Parameters
            ----------
            db               : Name of gridded population database
            
            country_iso3     : ISO standard (Alpha-3) for country name
            
            country_ENG_name : English country name 
            
            Returns
            -------
            Population databases by nation in utm projection
            """
            
            # Define folder to create 
            dirs = '../data/' + str(db) 
            # Create folder if does not already exist
            if not os.path.exists(dirs):    
                os.makedirs(dirs)
        
            # Read nations shapefile
            world = gpd.read_file(filename='../data/gadm/gadm36_levels.gpkg', layer = 'level0', crs='EPSG:4326')
                       
            if self.country_iso3 is not None:
                
                # Get list of unique nations by ISO 
                nations_unique = list(world['GID_0'].unique())
                # Convert list of nations to string for comparison
                nations_str = ", ".join(map(str, self.country_iso3))
            
                # Check nations listed in gpkg 
                for iso in self.country_iso3:
                    # Check if defined nations is in attributes of shapefile
                    if iso not in nations_unique:
                        raise ValueError('`Defined nation not available, check your spelling' \
                                         'The ones available are: %s only has population data for %s' % (nations_str, nations_unique))
                    
                # Loop through list of nations
                for iso in self.country_iso3:
     
                    # Subset country for downloading worldpop data
                    country = world[world['GID_0'] == iso].reset_index(drop=True)
                    # Extract extent of nation 
                    xmin,ymin,xmax,ymax = country.total_bounds
                    # Create dictionary, where key is English name and value is ISO name - important for getting data - see path 
                    iso_dict = country.set_index('NAME_0').to_dict()['GID_0']
                    # Extract value as lower case string
                    #iso_dict_country_lower = "".join(list(iso_dict.values())).lower()
                    # Extract value as upper case string
                    iso_dict_country_upper = "".join(list(iso_dict.values())) # Already upper
                    # Define output path
                    output = dirs + '/' + str(db) + '_' + str(iso_dict_country_upper) + '.tif'
                    
                    # Define file path to where tif is
                    if 'ghs_pop' == db:
                        tif = dirs + '/GHS_POP_E2015_GLOBE_R2019A_4326_30ss_V1_0.tif'
                        # Clip global tif using country extent 
                        system('gdalwarp -te %s %s %s %s -overwrite -co COMPRESS=LZW --config GDAL_CACHEMAX 2048 -co NUM_THREADS=ALL_CPUS -multi -of GTiff %s %s' %(xmin, ymin, xmax, ymax, tif, output))
            
                    elif 'gpw' == db:
                        tif = dirs + '/gpw_v4_population_density_adjusted_to_2015_unwpp_country_totals_rev11_2015_30_sec.tif'
                        # Clip global tif using country extent 
                        system('gdalwarp -te %s %s %s %s -overwrite -co COMPRESS=LZW --config GDAL_CACHEMAX 2048 -co NUM_THREADS=ALL_CPUS -multi -of GTiff %s %s' %(xmin, ymin, xmax, ymax, tif, output))
                        
            elif self.country_iso3 is None:
                
                # Get list of unique nations by ISO 
                nations_unique = list(world['NAME_0'].unique())
                # Convert list of nations to string for comparison
                print(self.country_name)
                nations_str = ", ".join(map(str, self.country_name))
                
                # Check nations listed in gpkg 
                for name in self.country_name:
                    # Check if defined nations is in attributes of shapefile
                    if name not in nations_unique:
                        raise ValueError('`Defined nation not available, check your spelling.' \
                                         'The ones available are: %s only has population data for %s' % (nations_str, nations_unique))
                    
                # Loop through list of nations
                for n in self.country_name:
     
                    # Subset country for downloading worldpop data
                    country = world[world['NAME_0'] == n].reset_index(drop=True)
                    # Extract extent of nation 
                    xmin,ymin,xmax,ymax = country.total_bounds
                    # Create dictionary, where key is English name and value is ISO name - important for getting data - see path 
                    iso_dict = country.set_index('NAME_0').to_dict()['GID_0']
                    # Extract value as lower case string
                    #iso_dict_country_lower = "".join(list(iso_dict.values())).lower()
                    # Extract value as upper case string
                    iso_dict_country_upper = "".join(list(iso_dict.values())) # Already upper
                    # Define output path
                    output = dirs + '/' + str(db) + '_' + str(iso_dict_country_upper) + '.tif'
                    
                    # Define file path to where tif is
                    if 'ghs_pop' == db:
                        tif = dirs + '/GHS_POP_E2015_GLOBE_R2019A_4326_30ss_V1_0.tif'
                        # Clip global tif using country extent 
                        system('gdalwarp -te %s %s %s %s -overwrite -co COMPRESS=LZW --config GDAL_CACHEMAX 2048 -co NUM_THREADS=ALL_CPUS -multi -of GTiff %s %s' %(xmin, ymin, xmax, ymax, tif, output))
            
                    elif 'gpw' == db:
                        tif = dirs + '/gpw_v4_population_density_adjusted_to_2015_unwpp_country_totals_rev11_2015_30_sec.tif'
                        # Clip global tif using country extent 
                        system('gdalwarp -te %s %s %s %s -overwrite -co COMPRESS=LZW --config GDAL_CACHEMAX 2048 -co NUM_THREADS=ALL_CPUS -multi -of GTiff %s %s' %(xmin, ymin, xmax, ymax, tif, output))
            

def nation_shp_to_postgis(schema, user, dbname):
    
    """
    Apply a function that uploads nation boundaries provided by ESRI to db 
    """
    # Data from https://www.arcgis.com/home/item.html?id=2ca75003ef9d477fb22db19832c9554f
    # Define tables 
    table = schema + '.countries'
    # Define path to countries shp
    path = '../data/countries_shp/countries.shp'
    # Define command to export shp to psql
    os.system("shp2pgsql -I -s 4326 ../data/countries_shp/countries.shp %s | psql -U %s -d %s -p 5432" %(table, user, dbname))
    # Execute command
    #os.system(cmd)
    
# Upload countries shapefile as table
nation_shp_to_postgis(schema, user, dbname)

    def worldpop(self):
        
        """
        Apply a function that downloads UN-adjusted WorldPop population data
                
        Parameters
        ----------
        None    : N/A
        
        """
        # Define output folder to store tif
        dirs = '../data/worldpop'
        # Create folder if does not already exist
        if not os.path.exists(dirs):    
            os.makedirs(dirs)
        
        # Read gadm
        world = gpd.read_file(filename = '../data/gadm/gadm36_levels.gpkg', layer = 'level0', crs = 'EPSG:4326')
        
        
        if self.country_iso3 is not None:
                
            # Get list of unique nations by ISO 
            nations_unique = list(world['GID_0'].unique())
            # Convert list of nations to string for comparison
            nations_str = ", ".join(map(str, self.country_iso3))
            
        
            # Loop through list of nations
            for n in self.country_iso3:
                # Check if defined nations is in attributes of shapefile
                if n not in nations_unique:
                    raise ValueError('`Defined nation not available, check your spelling. The ones available are: %s only has population data for %s' % (nations_str, nations_unique))
                    
                else: # Continue   
                    # Subset country for downloading worldpop data
                    country = world[world['GID_0'] == n].reset_index(drop = True)
                    # Create dictionary, where key is English name and value is ISO name - important for getting data - see path 
                    iso_dict = country.set_index('NAME_0').to_dict()['GID_0']
                    # Extract value as lower case string
                    iso_dict_country_lower = "".join(list(iso_dict.values())).lower()
                    # Extract value as upper case string
                    iso_dict_country_upper = "".join(list(iso_dict.values())) # Already upper

                    # Define path to download un-adjusted worldpop data 
                    path = 'https://data.worldpop.org/GIS/Population/Global_2000_2020/2015/%s/%s_ppp_2015.tif' \
                    %(iso_dict_country_upper, iso_dict_country_lower) 
                    print(path)
                    # Define tif file output name & file path
                    output = dirs + '/worldpop_' + str(iso_dict_country_upper) + '.tif'

                    # If worldpop data does not already exist in folder: download file
                    if not os.path.exists(output):   
                        system("wget -O %s %s --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 0" %(output, path))
                           
        elif self.country_iso3 is None:
            
            # Get list of unique nations by ISO 
            nations_unique = list(world['NAME_0'].unique())
            # Convert list of nations to string for comparison
            print(self.country_name)
            nations_str = ", ".join(map(str, self.country_name))                       
                        
            # Loop through list of nations
            for n in self.country_iso3:
                # Check if defined nations is in attributes of shapefile
                if n not in nations_unique:
                    raise ValueError('`Defined nation not available, check your spelling. The ones available are: %s only has population data for %s' % (nations_str, nations_unique))
                    
                else: # Continue   
                    # Subset country for downloading worldpop data
                    country = world[world['NAME_0'] == n].reset_index(drop = True)
                    # Create dictionary, where key is English name and value is ISO name - important for getting data - see path 
                    iso_dict = country.set_index('NAME_0').to_dict()['GID_0']
                    # Extract value as lower case string
                    iso_dict_country_lower = "".join(list(iso_dict.values())).lower()
                    # Extract value as upper case string
                    iso_dict_country_upper = "".join(list(iso_dict.values())) # Already upper

                    # Define path to download un-adjusted worldpop data 
                    path = 'https://data.worldpop.org/GIS/Population/Global_2000_2020/2015/%s/%s_ppp_2015.tif' \
                    %(iso_dict_country_upper, iso_dict_country_lower) 
                    # Define tif file output name & file path
                    output = dirs + '/worldpop_' + str(iso_dict_country_upper) + '.tif'

                    # If worldpop data does not already exist in folder: download file
                    if not os.path.exists(output):   
                        system("wget -O %s %s --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 0" %(output, path))
                     