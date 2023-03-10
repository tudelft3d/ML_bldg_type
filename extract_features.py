'''
Database functions.
Based on: Ellie Roy (https://github.com/ellieroy/no-floors-inference-NL)
& Imke Lansky (https://github.com/ImkeLansky/USA-BuildingHeightInference)
'''

import db_functions

#2D problems
def get_buildingfunction(cursor, table):

    print('\n>> Dataset {0} -- obtaining building function'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS uses VARCHAR[];")
    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS bag_function VARCHAR;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET uses = subquery.uses " + 
        "FROM " + 
            "(SELECT pandid, ARRAY_AGG(distinct gebruiksdoelen) AS uses " +
            "FROM verblijfsobject, unnest(pandref) AS pandid, unnest(gebruiksdoel) AS gebruiksdoelen " +
            "GROUP BY pandid) AS subquery " + 
        "WHERE training_data." + table + "_tmp.bag_id = subquery.pandid;" 
    )

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET bag_function = subquery.bag_function " + 
        "FROM " + 
            "(SELECT bag_id, " + 
            "CASE " + 
                "WHEN uses = '{woonfunctie}' THEN 'Residential' " + 
                "WHEN uses != '{woonfunctie}' AND 'woonfunctie' = ANY(uses) THEN 'Mixed-residential' " + 
                "WHEN 'woonfunctie' != ANY(uses) AND uses != '{overige gebruiksfunctie}' AND cardinality(uses) = 1 THEN 'Non-residential (single-function)' " + 
                "WHEN 'woonfunctie' != ANY(uses) AND uses != '{overige gebruiksfunctie}' AND cardinality(uses) > 1 THEN 'Non-residential (multi-function)' " + 
                "WHEN uses = '{overige gebruiksfunctie}' THEN 'Others' " + 
                "WHEN uses IS NULL THEN 'Unknown' " + 
            "END AS bag_function " +
            "FROM training_data." + table + "_tmp) AS subquery " + 
        "WHERE training_data." + table + "_tmp.bag_id = subquery.bag_id;"
    )

    cursor.execute(
        "ALTER TABLE training_data." + table + "_tmp " +
        "DROP COLUMN uses;"
    )
    return

def get_constructionyear(cursor, table):

    print('\n>> Dataset {0} -- obtaining construction year'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS bag_construction_year INTEGER;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET bag_construction_year = subquery.bag_construction_year " + 
        "FROM " + 
            "(SELECT identificatie, oorspronkelijkbouwjaar AS bag_construction_year " + 
            "FROM public.pand) AS subquery " +
        "WHERE training_data." + table + "_tmp.bag_id = subquery.identificatie;"
    )

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET bag_construction_year = NULL " + 
        "WHERE training_data." + table + "_tmp.bag_construction_year = 1005;"
    )

def get_num_dwellings(cursor, table):
    print('\n>> Dataset {0} -- obtaining number of dwellings'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS bag_no_dwellings INTEGER;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET bag_no_dwellings = subquery.bag_no_dwellings " + 
        "FROM " + 
            "(SELECT pandid, COUNT(DISTINCT identificatie) AS bag_no_dwellings " +
            "FROM verblijfsobject, unnest(pandref) AS pandid " +
            "GROUP BY pandid) AS subquery " + 
        "WHERE training_data." + table + "_tmp.bag_id = subquery.pandid;" 
    )
    return

def get_num_adjacent_bldg_w_residential():
    return

def get_num_adjacent_bldg_of_adjacent_bldg():
    return

def get_footprint(cursor, table): 

    print('\n>> Dataset {0} -- obtaining building footprint'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS footprint_geom GEOMETRY;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET footprint_geom = subquery.footprint_geom " + 
        "FROM " + 
            "(SELECT identificatie, wkb_geometry AS footprint_geom " + 
            "FROM public.pand) AS subquery " +
        "WHERE training_data." + table + "_tmp.bag_id = subquery.identificatie;"
    )

    #create index on footprint geometry column
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS " + table + "_footprint_idx_tmp " + 
        "ON training_data." + table + "_tmp " + 
        "USING GIST (footprint_geom);"
    )
    return

def get_fp_area(cursor, table):
    
    print('\n>> Dataset {0} -- obtaining building footprint area'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS fp_area REAL;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET fp_area = ST_Area(footprint_geom);"
    )
    return

def get_fp_perimeter(cursor, table):
    
    print('\n>> Dataset {0} -- obtaining building footprint perimeter'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS fp_perimeter REAL;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET fp_perimeter = ST_Perimeter(footprint_geom);"
    )
    return

def get_num_vertices(cursor, table):
    
    print('\n>> Dataset {0} -- obtaining number of footprint vertices'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS fp_no_vertices REAL;")
    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS fp_no_vertices_simple REAL;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET fp_no_vertices = ST_NPoints(footprint_geom);"
    )

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " + 
        "SET fp_no_vertices_simple = ST_NPoints(ST_SimplifyPreserveTopology(footprint_geom, 0.1));"
    )
    return

def get_num_neighbours():
    return

def get_mbr(cursor, table):

    print('\n>> Dataset {0} -- obtaining minimum bounding box of footprint'.format(table))

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS bbox GEOMETRY;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " +
        "SET bbox = ST_OrientedEnvelope(footprint_geom);"
    )

    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS side_1 DOUBLE PRECISION;")
    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS side_2 DOUBLE PRECISION;")

    # Store the length of the two sides of the MBR as well.
    cursor.execute(
        "UPDATE training_data." + table + "_tmp " +
        "SET side_1 = ST_Distance(ST_Point(ST_Xmin(bbox), ST_Ymin(bbox)), ST_Point(ST_Xmin(bbox), ST_Ymax(bbox))), " +
        "side_2 = ST_Distance(ST_Point(ST_Xmin(bbox), ST_Ymin(bbox)), ST_Point(ST_Xmax(bbox), ST_Ymin(bbox)));"
    )

    cursor.execute("ALTER TABLE training_data." + table + "_tmp DROP COLUMN bbox;")    

    return

def get_bldg_length_width(cursor, table):

    get_mbr(cursor, table)

    print('\n>> Dataset {0} -- computing footprint width and length'.format(table))
    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS fp_length DOUBLE PRECISION;")
    cursor.execute("ALTER TABLE training_data." + table + "_tmp ADD COLUMN IF NOT EXISTS fp_width DOUBLE PRECISION;")

    cursor.execute(
        "UPDATE training_data." + table + "_tmp " +
        "SET fp_length = subquery.length, fp_width = subquery.width" +
        " FROM (SELECT bag_id, CASE WHEN side_1 > side_2 THEN side_1 " +
        "ELSE side_2 END AS length, CASE WHEN side_1 < side_2 THEN side_1 " +
        "ELSE side_2 END AS width FROM training_data." + table + "_tmp) AS subquery " +
        "WHERE training_data." + table + "_tmp.bag_id = subquery.bag_id;")

    # Drop the helper columns.
    cursor.execute("ALTER TABLE training_data." + table + "_tmp DROP COLUMN side_1;")
    cursor.execute("ALTER TABLE training_data." + table + "_tmp DROP COLUMN side_2;")
    return

#3D problems
def get_bldg_height():
    return

def get_roofshape():
    return

def get_surface_areas():
    return

def get_bldg_volume():
    return

def main():
    table = 'c1_rh'

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #create temporary table to store extracted features in
    db_functions.create_temp_table(cursor, table, pkey='bag_id')

    #get building function and remove any rows where function is not residential/mixed-residential
    get_buildingfunction(cursor, table)
    print('\n>> Dataset {0} -- removing non-residential buildings'.format(table))
    cursor.execute("DELETE FROM training_data." + table + "_tmp WHERE bag_function != 'Residential' AND bag_function != 'Mixed-residential';")

    #get features
    get_constructionyear(cursor, table)
    get_num_dwellings(cursor, table)
    get_num_adjacent_bldg_w_residential() #not implemented yet
    get_num_adjacent_bldg_of_adjacent_bldg() #not implemented yet
    get_footprint(cursor,table) #needed for other features
    get_fp_area(cursor,table)
    get_fp_perimeter(cursor,table)
    get_num_vertices(cursor,table)
    get_num_neighbours() #not implemented yet
    get_bldg_length_width(cursor, table)


    #close db connection
    db_functions.close_connection(conn, cursor)

    return

if __name__ == '__main__':
    main()