import csv, sys, os
import psycopg2 as psql
from psycopg2 import sql
from ..config.config import * 

def psql_version(section = 'postgresql', config_path = './config/', config_file = 'database.ini'):
    
    """ Check PostgreSQL database server is running and version """
    
    conn = None
    
    try:
        # Define db_params
        dbParams = config(section, config_path, config_file)
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        # Open connection
        conn = psql.connect(**dbParams)
        
        # create a cursor
        curr = conn.cursor()
        
        # execute a statement
        print('PostgreSQL database version:')
        curr.execute('SELECT version()')
        
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

def postgis_version(section = 'postgresql', config_path = './config/', config_file = 'database.ini'):
    
    """ Check postgis version setup on postgreSQL database server """
    
    conn = None
    
    try:
        # Define db_params
        dbParams = config(section, config_path, config_file)
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        # Open connection
        conn = psql.connect(**dbParams)
        
        # create a cursor
        curr = conn.cursor()
        
        # execute a statement
        print('Postgis version:')
        print(curr.execute('SELECT version()'))
        
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
