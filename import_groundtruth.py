import db_functions

def extract_rh_groundtruth():
    """
    Extract pand bag_id's and building types from Rijssen-Holten open energy testbed in 3DCityDB schemas
    """

    print('\n>> Extracting labeled data from energy testbed Rijssen-Holten')

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to 3dcity db
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #create table of labeled data (in training_data schema)
    cursor.execute('''
    CREATE SCHEMA IF NOT EXISTS training_data;
    DROP TABLE IF EXISTS training_data.c1_rh;
    CREATE TABLE training_data.c1_rh AS
    SELECT cityobject.gmlid as bag_id, cityobject_genericattrib.strval AS building_type
    FROM citydb.cityobject, citydb.cityobject_genericattrib
    WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'building_type'
    ORDER BY bag_id;
    ''')

    #close db connection
    db_functions.close_connection(conn, cursor)
    return

def extract_ep_groundtruth():
    """
    Extract pand bag_id's and building types from input_data."ep-online"
    """

    print('\n>> Extracting labeled data from ep-online')

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #create table of labeled data (in training_data schema)
    cursor.execute('''
    CREATE SCHEMA IF NOT EXISTS training_data;
    DROP TABLE IF EXISTS training_data.c2_ep;
    CREATE TABLE training_data.c2_ep AS
    SELECT 'NL.IMBAG.Pand.' || "Pand_bagpandid" AS bag_id, ARRAY_AGG("Pand_gebouwtype") AS building_type
    FROM input_data."ep-online"
    WHERE "Pand_status" = 'Bestaand' AND "Pand_gebouwtype" IS NOT NULL AND "Pand_bagpandid" IS NOT NULL
    GROUP BY bag_id
    ORDER BY bag_id ASC;
    ''')

    #close db connection
    db_functions.close_connection(conn, cursor)
    return

def compare_groundtruth():

    print('\n>> Creating table to compare labeled data from ep-online to Rijssen-Holten energy testbed')

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #create table of labeled data (in training_data schema)
    cursor.execute('''
    CREATE SCHEMA IF NOT EXISTS training_data;
    DROP TABLE IF EXISTS training_data.comparison;
    CREATE TABLE training_data.comparison AS
    SELECT c1_rh.bag_id, c1_rh.building_type AS rh_building_type, c2_ep.building_type AS ep_building_type
    FROM training_data.c1_rh, training_data.c2_ep
    WHERE c1_rh.bag_id = c2_ep.bag_id;
    ''')

    #close db connection
    db_functions.close_connection(conn, cursor)
    return

def main():
    extract_rh_groundtruth()
    extract_ep_groundtruth()
    compare_groundtruth()

if __name__ == '__main__':
    main()