import os
import db_functions

def extract_rh_groundtruth():
    """
    Need to clean data still
    """
    #3dcity database name
    database3dcity = 'geo2020_RHe_test'

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to 3dcity db
    conn = db_functions.setup_connection(user,password,database3dcity,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #create table with labeled data
    cursor.execute('''DROP TABLE IF EXISTS public.c1_rh;
    CREATE TABLE public.c1_rh AS
    SELECT cityobject.gmlid as bag_id, cityobject_genericattrib.strval AS building_type
    FROM citydb.cityobject, citydb.cityobject_genericattrib
    WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'building_type'
    ORDER BY bag_id;''')
    
    #create dump file of created table and load it in target db
    os.system('pg_dump -t public.c1_rh {0} | psql {1}'.format(database3dcity,database))

    #drop the created table
    cursor.execute("DROP TABLE IF EXISTS public.c1_rh;")

    #close 3dcity db connection
    db_functions.close_connection(conn, cursor)

    #create connection to db
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #move from public schema to training_data schema
    cursor.execute('''CREATE SCHEMA IF NOT EXISTS training_data;
    ALTER TABLE public.c1_rh SET SCHEMA training_data;''')

    #close db connection
    db_functions.close_connection(conn, cursor)

    return

def extract_ep_groundtruth():
    """
    Need to format pand id
    One pand id can have multiple verblijfsobject id, each with their own building type -> not sure, how to handle this
    """
    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #create table of labeled data (in training_data schema)
    cursor.execute('''CREATE SCHEMA IF NOT EXISTS training_data;
    DROP TABLE IF EXISTS training_data.c2_ep;
    CREATE TABLE training_data.c2_ep AS
    SELECT index, "Pand_bagpandid", "Pand_gebouwtype"
    FROM public."ep-online"
    WHERE "Pand_status" = 'Bestaand' AND "Pand_gebouwtype" IS NOT NULL AND "Pand_bagpandid" IS NOT NULL 
    ORDER BY index ASC;''')

    #close db connection
    db_functions.close_connection(conn, cursor)

    return

def main():
    extract_rh_groundtruth()
    extract_ep_groundtruth()

if __name__ == '__main__':
    main()