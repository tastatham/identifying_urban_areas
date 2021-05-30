#!/usr/bin/python

# Import libraries
import os
try:
    import configparser
except:
    from six.moves import configparser
from pathlib import Path
import psycopg2 as psql

_config = dict(
        db_host = 'localhost',
        db_port = 5432,
        db_database = 'comparing_urban',
        db_user = 'postgres',
        db_password =  'postgres',
        db_schema = 'urban_pop'
        )

def config(section = 'postgresql', config_path = '../src/config/', config_file = 'database.ini'):
    
    global db, parser
    
    # Define path 
    path = str(config_path) + str(config_file)
    # Define parser
    parser = configparser.ConfigParser()
    # Read file
    parser.read(path)
    # Creaty empty dict
    db = {}
    # Populate empty dict
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else: # Raise error
        raise Exception('Section {0} not found in the {1} file'.format(section, path))
        
    return db

def env_to_var():
    # Load defined env from .env file
    # Store env files as variable
    db_host = _config["db_host"]
    db_port = _config["db_port"]
    db_database = _config["db_database"]
    db_user = _config["db_user"]
    db_password = _config["db_password"]
    db_schema = _config["db_schema"]
    
    return db_host, db_port, db_database, db_user, db_password, db_schema
