'''
Database functions.
Based on: Ellie Roy (https://github.com/ellieroy/no-floors-inference-NL)
& Imke Lansky (https://github.com/ImkeLansky/USA-BuildingHeightInference)
'''

import db_functions
import json

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
        ALTER TABLE training_data.{table}_tmp DROP COLUMN uses;
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
            WHERE status LIKE 'Verblijfsobject in gebruik%' AND eindgeldigheid IS NULL
            GROUP BY pandid
            ORDER BY pandid ASC) AS subquery
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

def get_num_adjacent_bldg(cursor, table, buffer_size):
    """
    Get number of adjacent buildings (of all functions except for "Others" and "Unknown") of each building footprint and store results in the database. 
    Parameters:
    cursor -- cursor for database connection 
    table -- table to store the features in the database
    adjacent_distance -- list of distances to adjacent buildings
    Returns: none
    
    """

    # Compute buffer around footprints
    compute_buffers(cursor, table, buffer_size)
    
    print(f'\n>> Dataset {table} -- obtaining number of adjacent buildings from footprints')

    # Extract number of adjacent buildings based on buffer
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS no_adjacent_bldg INTEGER;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET no_adjacent_bldg = subquery.no_adjacent
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
        SET no_adjacent_bldg = 0 
        WHERE no_adjacent_bldg IS NULL
        AND footprint_geom IS NOT NULL;
        '''
    )
    return

def get_num_adjacent_bldg_of_adjacent_bldg(cursor, table):
    """
    Get (maximum) number of adjacent buildings of adjacent buildings
    """

    print(f'\n>> Dataset {table} -- obtaining number of adjacent buildings of adjacent building(s)')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS no_adjacent_of_adja_bldg INTEGER;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET no_adjacent_of_adja_bldg = subquery.no_adjacent_of_adja_bldg
        FROM
            (SELECT a.bag_id, MAX(b.no_adjacent_bldg) AS no_adjacent_of_adja_bldg
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
                (SELECT a.bag_id, COUNT(*) AS no_neighbours, ARRAY_AGG(b.bag_id)
                FROM training_data.{table}_tmp AS a
                JOIN training_data.{table}_tmp AS b
                ON ST_DWithin(a.footprint_centroid, b.footprint_geom, {dist})
                WHERE a.bag_id != b.bag_id
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

# def get_rooftype(cursor, table):

#     print(f'\n>> Dataset {table} -- obtaining roof type')

#     cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS roof_type VARCHAR;")

#     cursor.execute(f'''
#         UPDATE training_data.{table}_tmp
#         SET roof_type = subquery.roof_type
#         FROM
#             (SELECT cityobject.gmlid AS bag_id, cityobject_genericattrib.strval AS roof_type
#             FROM citydb2.cityobject, citydb2.cityobject_genericattrib
#             WHERE attrname = 'dak_type' AND cityobject.id = cityobject_genericattrib.cityobject_id) AS subquery
#         WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
#         '''
#     )
#     return

#3D problems
def get_3DBM_features(cursor, table, lod):

    print(f'\n>> Dataset {table} -- obtaining {lod} 3DBM features')

    #Add new column to format bag id <- REMINDER: THIS MAKE IT DOES NOT TAKE BUILDINGS WITH UNDERGROUND PARTS INTO ACCOUNT
    cursor.execute(f"ALTER TABLE input_data.{table}_{lod}_3dbm ADD COLUMN IF NOT EXISTS new_id VARCHAR")

    cursor.execute(f'''
        UPDATE input_data.{table}_{lod}_3dbm
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
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS height_max_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS height_min_roof_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS shared_walls_area_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS closest_distance_{lod} DOUBLE PRECISION;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET actual_volume_{lod} = {table}_{lod}_3dbm.actual_volume_{lod},
        convex_hull_volume_{lod} = {table}_{lod}_3dbm.convex_hull_volume_{lod},
        footprint_perimeter_{lod} = {table}_{lod}_3dbm.footprint_perimeter_{lod},
        obb_width_{lod} = {table}_{lod}_3dbm.obb_width_{lod},
        obb_length_{lod} = {table}_{lod}_3dbm.obb_length_{lod},
        ground_area_{lod} = {table}_{lod}_3dbm.ground_area_{lod},
        wall_area_{lod} = {table}_{lod}_3dbm.wall_area_{lod},
        roof_area_{lod} = {table}_{lod}_3dbm.roof_area_{lod},
        ground_point_count_{lod} = {table}_{lod}_3dbm.ground_point_count_{lod},
        height_max_{lod} = {table}_{lod}_3dbm."max_Z_{lod}" - {table}_{lod}_3dbm."ground_Z_{lod}",
        height_min_roof_{lod} = {table}_{lod}_3dbm."min_Z_{lod}" - {table}_{lod}_3dbm."ground_Z_{lod}",    
        shared_walls_area_{lod} = {table}_{lod}_3dbm.shared_walls_area_{lod},
        closest_distance_{lod} = {table}_{lod}_3dbm.closest_distance_{lod}
        FROM input_data.{table}_{lod}_3dbm
        WHERE training_data.{table}_tmp.bag_id = input_data.{table}_{lod}_3dbm.new_id;
        '''
    )

    cursor.execute(f'''
        ALTER TABLE input_data.{table}_{lod}_3dbm DROP COLUMN new_id;
        '''
    )
    return

#data cleaning
def remove_redundant_features(cursor, table):

    print(f'\n>> Dataset {table} -- removing redundant features')

    #fp_area, ground_area_lod1, ground_area_lod2 -> ground_area_lod1 and ground_area_lod2 are (mostly) the same
    #however there are differences in fp_area and ground_area_lod1 ranging from -1519 to 1671.82, for now fp_area is taken since it's from BAG
    #Remove ground_area_lod1 and ground_area_lod2
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS ground_area_lod1;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS ground_area_lod2;")

    #fp_perimeter, footprint_perimeter_lod1, footprint_perimeter_lod2 -> footprint_perimeter_lod1 and footprint_perimeter_lod2 are (mostly) the same
    #however there are differences in fp_perimeter and footprint_perimeter_lod1 ranging from -164 to 110, for now fp_perimeter is taken since it's from BAG
    #Remove footprint_perimeter_lod1 and footprint_perimeter_lod2
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS footprint_perimeter_lod1;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS footprint_perimeter_lod2;")

    #fp_no_vertices is taken since it's from BAG
    #Remove fp_no_vertices_simple, ground_point_count_lod1 and ground_point_count_lod2.
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS fp_no_vertices_simple;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS ground_point_count_lod1;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS ground_point_count_lod2;")

    #obb_width_lod1 and obb_width_lod2 are the same, same for obb_length_lod1 and obb_length_lod2
    #obb_width and obb_length are taken, after closer inspection of the footprint and the width and length values (also because they are from 3D BAG)
    #Remove fp_length and fp_width
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS fp_length;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS fp_width;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS obb_width_lod2;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS obb_length_lod2;")


    #Remove bag_function, used to filter out non-residential buildings
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS bag_function;")

    #Remove footprint_geom, used for footprint functions
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS footprint_geom;")

    #height_min_roof_lod1 = height_max_lod1 -> LoD 1.2
    #Remove height_min_roof_lod1
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS height_min_roof_lod1;")

    #Shared_wall_area and closest_distance added to double check adjacency, however it does not filter Other and Unknown function buildings
    #Remove shared_wall_area_lod1, shared_wall_area_lod2, closest_distance_lod1 and closest_distance_lod2
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS shared_walls_area_lod1;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS shared_walls_area_lod2;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS closest_distance_lod1;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS closest_distance_lod2;")

    #dropped roof_type, worse performer from the features and complicated the code since it was the only categorical feature
    # REMOVED cursor.execute(f"ALTER TABLE training_data.{table}_tmp DROP COLUMN IF EXISTS roof_type;")
    return

def remove_null_values(cursor, table):

    print(f'\n>> Dataset {table} -- removing rows with NULL values')

    cursor.execute(f'''
        DELETE FROM training_data.{table}_tmp
        WHERE building_type IS NULL OR
        actual_volume_lod1 IS NULL OR
        convex_hull_volume_lod1 IS NULL OR
        obb_width_lod1 IS NULL OR
        obb_length_lod1 IS NULL OR
        wall_area_lod1 IS NULL OR
        roof_area_lod1 IS NULL OR
        height_max_lod1 IS NULL OR
        actual_volume_lod2 IS NULL OR
        convex_hull_volume_lod2 IS NULL OR
        wall_area_lod2 IS NULL OR
        roof_area_lod2 IS NULL OR
        height_max_lod2 IS NULL OR
        height_min_roof_lod2 IS NULL;
        '''
    )
    return

def main():
    with open('params.json', 'r') as f:
        params = json.load(f)
        
        table = params['table']
        buffer_size = params['buffer_size']

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    #create temporary table to store extracted features in
    db_functions.create_temp_table(cursor, table, pkey='bag_id')

    #get building function
    get_buildingfunction(cursor, table)

    #needed for other features
    get_footprint(cursor, table)

    #get adjacent number of buildings (before removing non-residential)
    get_num_adjacent_bldg(cursor, table, buffer_size)
    get_num_adjacent_bldg_of_adjacent_bldg(cursor, table)
    get_num_neighbours(cursor, table, [25, 50, 75, 100])

    #remove any rows where function is not residential/mixed-residential
    print(f'\n>> Dataset {table} -- removing non-residential buildings')
    cursor.execute(f"DELETE FROM training_data.{table}_tmp WHERE bag_function != 'Residential' AND bag_function != 'Mixed-residential';")

    #get 2D features
    get_constructionyear(cursor, table)
    get_num_dwellings(cursor, table)
    get_fp_area(cursor,table)
    get_fp_perimeter(cursor,table)
    get_num_vertices(cursor,table)
    get_bldg_length_width(cursor, table)
    # get_rooftype(cursor, table)

    #get 3D features
    lod1 = 'lod1'
    lod2 = 'lod2'
    get_3DBM_features(cursor, table, lod1)
    get_3DBM_features(cursor, table, lod2)

    #Clean data
    remove_redundant_features(cursor, table)
    remove_null_values(cursor, table)

    #Replace original table with the temporary table containing the feature candidates
    db_functions.replace_temp_table(cursor, table)

    #close db connection
    db_functions.close_connection(conn, cursor)

    return

if __name__ == '__main__':
    main()