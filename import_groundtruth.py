import db_functions
import json

def extract_rh_groundtruth():
    """
    Extract pand bag_id's and building types from Rijssen-Holten open energy test-bed in 3DCityDB schemas
    """

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to 3dcity db
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    print('\n>> Extracting labelled data from energy test-bed Rijssen-Holten to table training_data.c1_rh')

    #create table with bag_id's of buildings in case study
    #NL.IMBAG.Pand.0150100000059983 is manually removed, because its footprint geom in the BAG is faulty
    cursor.execute('''
        CREATE SCHEMA IF NOT EXISTS training_data;
        DROP TABLE IF EXISTS training_data.c1_rh;
        CREATE TABLE training_data.c1_rh AS
        SELECT gmlid AS bag_id
        FROM citydb.cityobject
        WHERE objectclass_id = 26 AND name IS NOT NULL AND gmlid != 'NL.IMBAG.Pand.0150100000059983';
        '''
    )

    #add labelled data
    cursor.execute("ALTER TABLE training_data.c1_rh ADD COLUMN IF NOT EXISTS building_type VARCHAR;")
    cursor.execute('''
        UPDATE training_data.c1_rh
        SET building_type = subquery.building_type
        FROM
            (SELECT cityobject.gmlid as bag_id, cityobject_genericattrib.strval AS building_type
            FROM citydb.cityobject, citydb.cityobject_genericattrib
            WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'dutch_building_type'
            ORDER BY bag_id) AS subquery
        WHERE training_data.c1_rh.bag_id = subquery.bag_id
        '''
    )

    #close db connection
    db_functions.close_connection(conn, cursor)
    return

def extract_ep_groundtruth():
    """
    Extract pand bag_id's and building types from input_data."ep-online"
    """

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    print('\n>> Extracting labelled data from ep-online to table training_data.c0_ep')

    #create table of labelled data (in training_data schema)
    #extracts the building type from each verblijfsobject in ep-online then linked with verblijfsobject (BAG) dataset to check its status
    #then linked to pandid via pandref (in verblijfsobject dataset)
    #the pandref is then also linked to pand (BAG) dataset to check its status as well
    cursor.execute('''
        CREATE SCHEMA IF NOT EXISTS training_data;
        DROP TABLE IF EXISTS training_data.c0_ep;
        CREATE TABLE training_data.c0_ep AS
        SELECT bag_id, ARRAY_AGG("ep-online"."Pand_gebouwtype") AS building_type
        FROM input_data."ep-online", input_data.verblijfsobject, unnest(pandref) AS bag_id, input_data.pand
        WHERE 'NL.IMBAG.Verblijfsobject.' || "Pand_bagverblijfsobjectid" = verblijfsobject.identificatie
        AND verblijfsobject.status LIKE 'Verblijfsobject in gebruik%'
        AND verblijfsobject.eindgeldigheid IS NULL
        AND bag_id = pand.identificatie
        AND pand.status LIKE 'Pand in gebruik%'
        AND pand.eindgeldigheid IS NULL
        GROUP BY bag_id
        ORDER BY bag_id ASC;
        '''
    )

    #close db connection
    db_functions.close_connection(conn, cursor)
    return

def get_groundtruth(table, citydbx):

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    print(f'\n>> Creating table {table} with bag_ids from {citydbx} and extract labelled data from c0_ep')

    #create table with bag_id's of buildings in case study
    cursor.execute(f'''
        CREATE SCHEMA IF NOT EXISTS training_data;
        DROP TABLE IF EXISTS training_data.{table};
        CREATE TABLE training_data.{table} AS
        SELECT gmlid AS bag_id
        FROM {citydbx}.cityobject
        WHERE objectclass_id = 26;
        '''
    )

    #add labelled data
    cursor.execute(f"ALTER TABLE training_data.{table} ADD COLUMN IF NOT EXISTS building_type text[];")
    cursor.execute(f'''
        UPDATE training_data.{table}
        SET building_type = c0_ep.building_type
        FROM training_data.c0_ep
        WHERE {table}.bag_id = training_data.c0_ep.bag_id
        '''
    )

    #close db connection
    db_functions.close_connection(conn, cursor)
    return

def main():
    with open('params.json', 'r') as f:
        params = json.load(f)
        
        table = params['table']
        citydbx = params['citydbx']

    #extract_ep_groundtruth()
    
    if table == 'c1_rh' and citydbx == 'citydb':
        extract_rh_groundtruth()
        return
    else:
        get_groundtruth(table, citydbx)
        return

if __name__ == '__main__':
    main()