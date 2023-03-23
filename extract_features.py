'''
Database functions.
Based on: Ellie Roy (https://github.com/ellieroy/no-floors-inference-NL)
& Imke Lansky (https://github.com/ImkeLansky/USA-BuildingHeightInference)
'''

import db_functions

#2D problems
def get_buildingfunction(cursor, table):

    print(f'\n>> Dataset {table} -- obtaining building function')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS uses VARCHAR[];")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS bag_function VARCHAR;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET uses = subquery.uses 
        FROM 
            (SELECT pandid, ARRAY_AGG(distinct gebruiksdoelen) AS uses
            FROM input_data.verblijfsobject, unnest(pandref) AS pandid, unnest(gebruiksdoel) AS gebruiksdoelen
            GROUP BY pandid) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.pandid;
        '''
    )

    cursor.execute(
        f"UPDATE training_data.{table}_tmp " + 
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
            f"FROM training_data.{table}_tmp) AS subquery " + 
        f"WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;"
    )

    cursor.execute(f'''
        ALTER TABLE training_data.{table}_tmp
        DROP COLUMN uses;
        '''
    )
    return

def get_constructionyear(cursor, table):

    print(f'\n>> Dataset {table} -- obtaining construction year')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS bag_construction_year INTEGER;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET bag_construction_year = subquery.bag_construction_year
        FROM
            (SELECT identificatie, oorspronkelijkbouwjaar AS bag_construction_year
            FROM input_data.pand) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.identificatie;
        '''
    )

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET bag_construction_year = NULL
        WHERE training_data.{table}_tmp.bag_construction_year = 1005;
        '''
    )
    return

def get_num_dwellings(cursor, table):

    print(f'\n>> Dataset {table} -- obtaining number of dwellings')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS bag_no_dwellings INTEGER;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET bag_no_dwellings = subquery.bag_no_dwellings
        FROM
            (SELECT pandid, COUNT(DISTINCT identificatie) AS bag_no_dwellings
            FROM input_data.verblijfsobject, unnest(pandref) AS pandid
            GROUP BY pandid) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.pandid;
        '''
    )
    return

def compute_buffers(cursor, table, size):
    """
    Compute buffers around all footprint geometries and store results in database. 
    
    Parameters:
    cursor -- cursor for database connection
    table -- table to store the features in the database
    size -- size of buffer (meters)
    Returns: none
    """

    print(f'\n>> Dataset {table} -- computing buffers of {size}m around footprints')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS footprint_buffer GEOMETRY;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET footprint_buffer = subquery.buffer
        FROM
            (SELECT bag_id, ST_Buffer(footprint_geom, {size}, 'join=mitre') AS buffer
            FROM training_data.{table}_tmp) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
        '''
    )

    cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS {table}_buf_idx_tmp
        ON training_data.{table}_tmp
        USING GIST (footprint_buffer);
        '''
    )
    return

def get_num_adjacent_bldg_w_residential(cursor, table, adjacent_distance):
    """
    Get number of adjacent buildings of each building footprint and store results in the database. 
    Parameters:
    cursor -- cursor for database connection 
    table -- table to store the features in the database
    adjacent_distance -- list of distances to adjacent buildings
    Returns: none
    
    """

    # Compute buffer around footprints
    compute_buffers(cursor, table, adjacent_distance)
    
    print(f'\n>> Dataset {table} -- obtaining number of adjacent residential buildings from footprints')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS no_adjacent_resi_bldg INTEGER;")

    # Extract number of adjacent buildings based on buffer
    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET no_adjacent_resi_bldg = subquery.no_adjacent
        FROM
            (SELECT a.bag_id, COUNT(*) AS no_adjacent
            FROM training_data.{table}_tmp AS a
            JOIN training_data.{table}_tmp AS b ON ST_INTERSECTS(a.footprint_buffer, b.footprint_geom)
            WHERE a.bag_id != b.bag_id
            AND a.bag_function != 'Others' AND a.bag_function != 'Unknown' 
            AND b.bag_function != 'Others' AND b.bag_function != 'Unknown' 
            GROUP BY a.bag_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
        '''
    )
    
    # Set number of adjacent buildings equal to zero when column is null
    # (except from when footprint geometry is equal to null)
    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET no_adjacent_resi_bldg = 0 
        WHERE no_adjacent_resi_bldg IS NULL
        AND footprint_geom IS NOT NULL;
        '''
    )
    return

def get_num_adjacent_bldg_of_adjacent_bldg(cursor, table):
    """
    Get (maximum) number of adjacent residential buildings of adjacent residential buildings
    """

    print(f'\n>> Dataset {table} -- obtaining number of adjacent buildings of adjacent building(s)')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS no_adjacent_of_adja_bldg INTEGER;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET no_adjacent_of_adja_bldg = subquery.no_adjacent_of_adja_bldg
        FROM
            (SELECT a.bag_id, MAX(b.no_adjacent_resi_bldg) AS no_adjacent_of_adja_bldg
            FROM training_data.{table}_tmp AS a
            JOIN training_data.{table}_tmp AS b ON ST_INTERSECTS(a.footprint_buffer, b.footprint_geom)
            WHERE a.bag_id != b.bag_id
            GROUP BY a.bag_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
        '''
    )

    # Set number of adjacent buildings of adjacent buildings equal to zero when column is null
    # (except from when footprint geometry is equal to null)
    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET no_adjacent_of_adja_bldg = 0 
        WHERE no_adjacent_of_adja_bldg IS NULL
            AND footprint_geom IS NOT NULL;
        '''
    )

    # Drop the buffer column
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN footprint_buffer;")
    return

def get_footprint(cursor, table): 

    print(f'\n>> Dataset {table} -- obtaining building footprint')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS footprint_geom GEOMETRY;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET footprint_geom = subquery.footprint_geom
        FROM
            (SELECT identificatie, wkb_geometry AS footprint_geom
            FROM input_data.pand) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.identificatie;
        '''
    )

    #create index on footprint geometry column
    cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS {table}_footprint_idx_tmp
        ON training_data.{table}_tmp
        USING GIST (footprint_geom);
        '''
    )
    return

def get_fp_area(cursor, table):
    
    print(f'\n>> Dataset {table} -- obtaining building footprint area')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS fp_area DOUBLE PRECISION;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET fp_area = ST_Area(footprint_geom);
        '''
    )
    return

def get_fp_perimeter(cursor, table):
    
    print(f'\n>> Dataset {table} -- obtaining building footprint perimeter')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS fp_perimeter DOUBLE PRECISION;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET fp_perimeter = ST_Perimeter(footprint_geom);
        '''
    )
    return

def get_num_vertices(cursor, table):
    
    print(f'\n>> Dataset {table} -- obtaining number of footprint vertices')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS fp_no_vertices INTEGER;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS fp_no_vertices_simple INTEGER;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp 
        SET fp_no_vertices = ST_NPoints(footprint_geom);
        '''
    )

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET fp_no_vertices_simple = ST_NPoints(ST_SimplifyPreserveTopology(footprint_geom, 0.1));
        '''
    )
    return

def compute_centroids(cursor, table): 
    """
    Compute centroids of all footprint geometries and store results in database. 
    
    Parameters: 
    cursor -- cursor for database connection
    table -- table to store the features in the database
    Returns: none
     """

    print(f'\n>> Dataset {table} -- computing centroids of footprints')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS footprint_centroid GEOMETRY;")   

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET footprint_centroid = subquery.centroid
        FROM
            (SELECT bag_id, ST_Centroid(footprint_geom) as centroid
            FROM training_data.{table}_tmp) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
        '''
    )

    cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS {table}_centroid_idx_tmp
        ON training_data.{table}_tmp
        USING GIST (footprint_centroid);
        '''
    )
    return

def get_num_neighbours(cursor, table, neighbour_distances): 
    """
    Get number of neighbouring buildings at different distances from 
    each building footprint centroid and store results in the database. 
    Parameters:
    cursor -- cursor for database connection
    table -- table to store the features in the database
    neighbour_distances -- list of distances to neighbouring building centroids
    Returns: none
    """

    # Compute footprint centroid 
    compute_centroids(cursor, table)

    print(f'\n>> Dataset {table} -- obtaining number of neighbouring buildings from these distances in m: {neighbour_distances}')

    # Extract number of neightbours based on centroid 
    for dist in neighbour_distances: 
        cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS no_neighbours_{dist}m INTEGER;")

        cursor.execute(f'''
            UPDATE training_data.{table}_tmp
            SET no_neighbours_{dist}m = subquery.no_neighbours
            FROM
                (SELECT a.bag_id, COUNT(*) AS no_neighbours
                FROM training_data.{table}_tmp AS a
                JOIN input_data.pand AS b
                ON ST_DWithin(a.footprint_centroid, b.wkb_geometry, {dist})
                WHERE a.bag_id != b.identificatie
                GROUP BY a.bag_id) AS subquery
            WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
            '''
        )

        # Set number of neighbouring buildings equal to zero when column is null
        # (except from when footprint geometry is equal to null)
        cursor.execute(f'''
            UPDATE training_data.{table}_tmp
            SET no_neighbours_{dist}m = 0
            WHERE no_neighbours_{dist}m IS NULL
            AND footprint_geom IS NOT NULL;
            '''
        )
    
    # Drop the centroid column
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN footprint_centroid;")
    return

def get_mbr(cursor, table):

    print(f'\n>> Dataset {table} -- obtaining minimum bounding box of footprint')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS bbox GEOMETRY;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET bbox = ST_OrientedEnvelope(footprint_geom);
        '''
    )

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS side_1 DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS side_2 DOUBLE PRECISION;")

    # Store the length of the two sides of the MBR as well.
    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET side_1 = ST_Distance(ST_Point(ST_Xmin(bbox), ST_Ymin(bbox)), ST_Point(ST_Xmin(bbox), ST_Ymax(bbox))),
        side_2 = ST_Distance(ST_Point(ST_Xmin(bbox), ST_Ymin(bbox)), ST_Point(ST_Xmax(bbox), ST_Ymin(bbox)));
        '''
    )

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN bbox;")    
    return

def get_bldg_length_width(cursor, table):

    get_mbr(cursor, table)

    print(f'\n>> Dataset {table} -- obtaining footprint width and length')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS fp_length DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS fp_width DOUBLE PRECISION;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET fp_length = subquery.length, fp_width = subquery.width
        FROM
            (SELECT bag_id, CASE WHEN side_1 > side_2 THEN side_1
            ELSE side_2 END AS length, CASE WHEN side_1 < side_2 THEN side_1
            ELSE side_2 END AS width FROM training_data.{table}_tmp) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
        '''
    )

    # Drop the helper columns.
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN side_1;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN side_2;")
    return

def get_rooftype(cursor, table):

    print(f'\n>> Dataset {table} -- obtaining roof type')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS roof_type VARCHAR;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET roof_type = subquery.roof_type
        FROM
            (SELECT cityobject.gmlid AS bag_id, cityobject_genericattrib.strval AS roof_type
            FROM citydb.cityobject, citydb.cityobject_genericattrib
            WHERE attrname = 'dak_type' AND cityobject.id = cityobject_genericattrib.cityobject_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
        '''
    )
    return

#3D problems
def get_3DBM_features(cursor, table, lod):

    print(f'\n>> Dataset {table} -- obtaining {lod} 3DBM features')

    #Add new column to format bag id
    cursor.execute(f"ALTER TABLE input_data.{lod}_3dbm ADD COLUMN IF NOT EXISTS new_id VARCHAR")

    cursor.execute(f'''
        UPDATE input_data.{lod}_3dbm
        SET new_id = LEFT(id, -2)
        '''
    )

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS actual_volume_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS convex_hull_volume_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS footprint_perimeter_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS obb_width_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS obb_length_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS ground_area_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS wall_area_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS roof_area_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS ground_point_count_{lod} INTEGER;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS max_z_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS min_z_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS ground_z_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS shared_walls_area_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS closest_distance_{lod} DOUBLE PRECISION;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET actual_volume_{lod} = {lod}_3dbm.actual_volume_{lod},
        convex_hull_volume_{lod} = {lod}_3dbm.convex_hull_volume_{lod},
        footprint_perimeter_{lod} = {lod}_3dbm.footprint_perimeter_{lod},
        obb_width_{lod} = {lod}_3dbm.obb_width_{lod},
        obb_length_{lod} = {lod}_3dbm.obb_length_{lod},
        ground_area_{lod} = {lod}_3dbm.ground_area_{lod},
        wall_area_{lod} = {lod}_3dbm.wall_area_{lod},
        roof_area_{lod} = {lod}_3dbm.roof_area_{lod},
        ground_point_count_{lod} = {lod}_3dbm.ground_point_count_{lod},
        max_z_{lod} = {lod}_3dbm."max_Z_{lod}",
        min_z_{lod} = {lod}_3dbm."min_Z_{lod}",
        ground_z_{lod} = {lod}_3dbm."ground_Z_{lod}",
        shared_walls_area_{lod} = {lod}_3dbm.shared_walls_area_{lod},
        closest_distance_{lod} = {lod}_3dbm.closest_distance_{lod}
        FROM input_data.{lod}_3dbm
        WHERE training_data.{table}_tmp.bag_id = input_data.{lod}_3dbm.new_id;
        '''
    )

    cursor.execute(f'''
        ALTER TABLE input_data.{lod}_3dbm
        DROP COLUMN new_id;
        '''
    )
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
    print(f'\n>> Dataset {table} -- removing non-residential buildings')
    cursor.execute(f"DELETE FROM training_data.{table}_tmp WHERE bag_function != 'Residential' AND bag_function != 'Mixed-residential';")

    #get 2D features
    get_constructionyear(cursor, table)
    get_num_dwellings(cursor, table)
    get_footprint(cursor,table) #needed for other features
    get_fp_area(cursor,table)
    get_fp_perimeter(cursor,table)
    get_num_vertices(cursor,table)
    get_bldg_length_width(cursor, table)
    get_num_adjacent_bldg_w_residential(cursor, table, 0.1)
    get_num_adjacent_bldg_of_adjacent_bldg(cursor, table)
    get_num_neighbours(cursor, table, [25, 50, 75, 100])
    get_rooftype(cursor, table)

    #get 3D features
    lod1 = 'lod1'
    lod2 = 'lod2'
    get_3DBM_features(cursor, table, lod1)
    get_3DBM_features(cursor, table, lod2)

    #close db connection
    db_functions.close_connection(conn, cursor)

    return

if __name__ == '__main__':
    main()