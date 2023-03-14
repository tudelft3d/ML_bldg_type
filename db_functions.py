'''
Database functions.
Based on: Ellie Roy (https://github.com/ellieroy/no-floors-inference-NL)
& Imke Lansky (https://github.com/ImkeLansky/USA-BuildingHeightInference)
'''

import os
import psycopg2
import sys

def get_db_parameters():
    location = os.path.dirname(os.path.abspath(__file__))
    parameters_file = os.path.join(location,"db_parameters.txt")

    with open (parameters_file, 'rt') as myfile:
        txt = myfile.read()
        params = txt.split()
        user,password,database,host,port = params
    return user,password,database,host,port

def setup_connection(user,password,database,host,port):
    '''
    Set up connection to the given database
    Parameters:
    user --  username
    password -- password of user
    database -- database name
    host -- host address of database
    port -- port number of database
    '''
    try:
        print("\n>> Connecting to PostgreSQL database: {0}".format(database))
        return psycopg2.connect(database=database, user=user, password=password, host=host, port=port)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL;", error)
        sys.exit()


def close_connection(connection, cursor):
    """
    Close connection to the database and cursor used to perform queries.
    Parameters:
    connection -- database connection
    cursor -- cursor for database connection
    """

    if cursor:
        cursor.close()
        print("\n>> Cursor is closed")

    if connection:
        connection.close()
        print("\n>> PostgreSQL connection is closed")

def create_temp_table(cursor, table, pkey=None):
    """
    Create a temporary table to extract the features into as copy of original table.
    Parameters:
    cursor -- cursor for database connection
    table -- table to store the data in the database
    pkey -- column to create primary key on (optional)
    """

    print('\n>> Dataset {0} -- creating temporary unlogged table'.format(table))

    cursor.execute("DROP TABLE IF EXISTS training_data." + table + "_tmp;") # CASCADE?
    cursor.execute("CREATE UNLOGGED TABLE training_data." + table + "_tmp AS TABLE training_data." + table + ";")

    if pkey is not None:
        try:
            cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD PRIMARY KEY (" + pkey + ");")
        except Exception as error:
            print('\nError: {0}'.format(str(error)))
    else:
        pass

def replace_temp_table(cursor, table, pkey=None, geom_index=None):
    """
    Replace original table with temporary table containing extracted data, drop temporary table and create (optional) indexes on new table.
    Parameters:
    cursor -- cursor for database connection
    table -- table to store the data in the database
    pkey -- column to create primary key on (optional)
    geom_index -- column to create geometry index on (optional)
    Returns: none

    """

    print('\n>> Dataset {0} -- copying unlogged table to logged table'.format(table))
    cursor.execute("CREATE TABLE training_data." + table + "_new AS TABLE training_data." + table + "_tmp;")
    cursor.execute("DROP TABLE training_data." + table + ";")
    cursor.execute("ALTER TABLE training_data." + table + "_new RENAME TO " + table + ";")
    cursor.execute("DROP TABLE training_data." + table + "_tmp;")

    if pkey is not None:
        try:
            cursor.execute("ALTER TABLE training_data." + table + " ADD PRIMARY KEY (" + pkey + ");")
        except Exception as error:
            print('\nError: {0}'.format(str(error)))
    else:
        pass

    if geom_index is not None:
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS " + table + "_" + geom_index + "_idx ON training_data." + table + " USING GIST (" + geom_index + ");")
        except Exception as error:
            print('\nError: {0}'.format(str(error)))
    else:
        pass