'''
Functions to validate features
'''

import json
import db_functions
import extract_features

def create_temp_validation_table(cursor, table, pkey=None):
    
    print(f'\n>> Dataset {table} -- creating temporary unlogged table for validation of features')

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS training_data;")
    cursor.execute(f"DROP TABLE IF EXISTS training_data.{table}_tmp;") # CASCADE?
    cursor.execute(f'''
        CREATE UNLOGGED TABLE training_data.{table}_tmp AS
        SELECT gmlid AS bag_id
        FROM citydb.cityobject
        WHERE objectclass_id = 26 AND name IS NOT NULL AND gmlid != 'NL.IMBAG.Pand.0150100000059983';
        '''
    )

    if pkey is not None:
        try:
            cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD PRIMARY KEY ({pkey});")
        except Exception as error:
            print(f'\nError: {str(error)}')
    else:
        pass

    return

def validate_no_adjacent_bldg(cursor, table, buffer_size):
    #get building function
    extract_features.get_buildingfunction(cursor, table)

    #needed for other features
    extract_features.get_footprint(cursor, table)

    extract_features.get_num_adjacent_bldg(cursor, table, buffer_size)
    
    extract_features.get_num_adjacent_bldg_of_adjacent_bldg(cursor, table)

    #compare with using buffer 0.30
    #create new buffer
    size = 0.30
    print(f'\n>> Dataset {table} -- computing buffers of {size}m around footprints')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS footprint_buffer030 GEOMETRY;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET footprint_buffer030 = subquery.buffer
        FROM
            (SELECT bag_id, ST_Buffer(footprint_geom, {size}, 'join=mitre') AS buffer
            FROM training_data.{table}_tmp) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id;
        '''
    )

    cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS {table}_buf_idx_tmp
        ON training_data.{table}_tmp
        USING GIST (footprint_buffer030);
        '''
    )

    # Extract number of adjacent buildings based on buffer of 0.30m
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS no_adjacent_bldg030 INTEGER;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET no_adjacent_bldg030 = subquery.no_adjacent
        FROM
            (SELECT a.bag_id, COUNT(*) AS no_adjacent
            FROM training_data.{table}_tmp AS a
            JOIN training_data.{table}_tmp AS b ON ST_INTERSECTS(a.footprint_buffer030, b.footprint_geom)
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
        SET no_adjacent_bldg030 = 0 
        WHERE no_adjacent_bldg030 IS NULL
        AND footprint_geom IS NOT NULL;
        '''
    )
    return

def validate_no_neighbours(cursor, table, neighbour_distances):
    #visualise footprints inside the different distances
    extract_features.get_num_neighbours(cursor, table, neighbour_distances)
    return

def validate_volumes(cursor, table):

    print(f'\n>> Dataset {table} -- obtaining lod1.2 geometries')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS lod12_geom GEOMETRY;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET lod12_geom = subquery.volume
        FROM
            (SELECT c.gmlid, c.volume
            FROM (
                SELECT b.gmlid, a.volume
                FROM (
                    SELECT a.naam AS gmlid, volume
                    FROM (
                        SELECT DISTINCT LEFT(cityobject.gmlid, -2) AS naam, ARRAY_AGG(surface_geometry.solid_geometry) AS vol
                        FROM citydb2.cityobject, citydb2.surface_geometry, citydb2.building
                        WHERE building.lod1_solid_id IS NOT NULL
                        AND surface_geometry.solid_geometry IS NOT NULL
                        AND surface_geometry.id = building.lod1_solid_id
                        AND cityobject.id = surface_geometry.cityobject_id
                        GROUP BY naam) a, unnest(a.vol) AS volume
                    WHERE cardinality(a.vol) = 1) a
                RIGHT JOIN (
                    SELECT cityobject.gmlid, cityobject_genericattrib.strval
                    FROM citydb2.cityobject, citydb2.cityobject_genericattrib
                    WHERE cityobject_genericattrib.attrname = 'val3dity_codes_lod12' AND cityobject_genericattrib.strval = '[]'
                    AND cityobject.id = cityobject_genericattrib.cityobject_id) AS b
                ON a.gmlid = b.gmlid
                ) c
            ORDER BY gmlid) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.gmlid;
        '''
    )

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS actual_volume_lod1_postgis DOUBLE PRECISION;")

    print(f'\n>> Dataset {table} -- computing volume from lod1.2 geometries')

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET actual_volume_lod1_postgis = ST_Volume(ST_MakeSolid(lod12_geom))
        WHERE bag_id = 'NL.IMBAG.Pand.1742100000000001'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000002'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000004'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000005'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000006'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000007'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000008'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000009'
        OR bag_id = 'NL.IMBAG.Pand.1742100000000010'
        '''
    )

    print(f'\n>> Dataset {table} -- obtaining lod2.2 volumes from rh dataset')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS actual_volume_lod2_rh DOUBLE PRECISION;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET actual_volume_lod2_rh = subquery.lod2_volume
        FROM
            (SELECT cityobject.gmlid as bag_id, CAST(cityobject_genericattrib.strval AS DOUBLE PRECISION) AS lod2_volume
            FROM citydb.cityobject, citydb.cityobject_genericattrib
            WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'lod2_volume'
            ORDER BY bag_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id
        '''
    )

    return

def validate_obb(cursor, table):

    extract_features.get_footprint(cursor, table)

    print(f'\n>> Dataset {table} -- obtaining minimum bounding box of footprint')

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS bbox GEOMETRY;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET bbox = ST_OrientedEnvelope(footprint_geom);
        '''
    )

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS area_from_bbox DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS area_check DOUBLE PRECISION;")

    print(f'\n>> Dataset {table} -- Computing area from bbox')

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET area_from_bbox = ST_3DArea(bbox);
        '''
    )

    print(f'\n>> Dataset {table} -- Computing area from obb_width_lod1 * obb_length_lod1')

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET area_check = (obb_width_lod1 * obb_length_lod1)
        FROM training_data.c1_rh
        WHERE {table}_tmp.bag_id = c1_rh.bag_id;
        '''
    )

    return

def validate_surface_areas(cursor, table):

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS roof_area_lod1_postgis DOUBLE PRECISION;")

    print(f'\n>> Dataset {table} -- computing roof_area_lod1 from 3D BAG using PostGIS function')

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET roof_area_lod1_postgis = subquery.roof_area_lod1_postgis
        FROM
            (SELECT cityobject.gmlid, SUM(ST_Area(surface_geometry.geometry)) AS roof_area_lod1_postgis
            FROM citydb2.cityobject, citydb2.thematic_surface, citydb2.surface_geometry
            WHERE cityobject.id = thematic_surface.building_id AND
            thematic_surface.lod2_multi_surface_id = surface_geometry.root_id AND
            thematic_surface.objectclass_id = 33
            GROUP BY cityobject.gmlid
            ORDER BY cityobject.gmlid) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.gmlid
        '''
    )

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS roof_area_lod2_rh DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS wall_area_lod2_rh DOUBLE PRECISION;")

    print(f'\n>> Dataset {table} -- obtaining roof_area_lod2 and wall_area_lod2 from rh dataset')

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET roof_area_lod2_rh = subquery.roof_area_lod2_rh
        FROM
            (SELECT cityobject.gmlid, SUM(CAST(cityobject_genericattrib.strval AS DOUBLE PRECISION)) AS roof_area_lod2_rh
            FROM citydb.cityobject, citydb.thematic_surface, citydb.surface_geometry, citydb.cityobject_genericattrib
            WHERE cityobject.id = thematic_surface.building_id AND
            thematic_surface.lod2_multi_surface_id = surface_geometry.parent_id AND
            surface_geometry.cityobject_id = cityobject_genericattrib.cityobject_id AND
            cityobject_genericattrib.attrname = 'lod2_area' AND
            thematic_surface.objectclass_id = 33
            GROUP BY cityobject.gmlid
            ORDER BY cityobject.gmlid) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.gmlid
        '''
    )

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET wall_area_lod2_rh = subquery.wall_area_lod2_rh
        FROM
            (SELECT cityobject.gmlid, SUM(CAST(cityobject_genericattrib.strval AS DOUBLE PRECISION)) AS wall_area_lod2_rh
            FROM citydb.cityobject, citydb.thematic_surface, citydb.surface_geometry, citydb.cityobject_genericattrib
            WHERE cityobject.id = thematic_surface.building_id AND
            thematic_surface.lod2_multi_surface_id = surface_geometry.parent_id AND
            surface_geometry.cityobject_id = cityobject_genericattrib.cityobject_id AND
            cityobject_genericattrib.attrname = 'lod2_area' AND
            thematic_surface.objectclass_id = 34
            GROUP BY cityobject.gmlid
            ORDER BY cityobject.gmlid) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.gmlid
        '''
    )
    

    return

def validate_height_values(cursor, table):

    get_height_values_3DBM(cursor, table, 'lod1')
    get_height_values_3DBM(cursor, table, 'lod2')

    #check 3DBAG height attributes
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS h_dak_max DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS h_dak_min DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS h_maaiveld DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS h_dak_70p DOUBLE PRECISION;")

    print(f'\n>> Dataset {table} -- obtaining 3D BAG height values')

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET h_dak_max = subquery.h_dak_max
        FROM
            (SELECT cityobject.gmlid as bag_id, cityobject_genericattrib.realval AS h_dak_max
            FROM citydb2.cityobject, citydb2.cityobject_genericattrib
            WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'h_dak_max'
            ORDER BY bag_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id
        '''
    )

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET h_dak_min = subquery.h_dak_min
        FROM
            (SELECT cityobject.gmlid as bag_id, cityobject_genericattrib.realval AS h_dak_min
            FROM citydb2.cityobject, citydb2.cityobject_genericattrib
            WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'h_dak_min'
            ORDER BY bag_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id
        '''
    )

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET h_maaiveld = subquery.h_maaiveld
        FROM
            (SELECT cityobject.gmlid as bag_id, cityobject_genericattrib.realval AS h_maaiveld
            FROM citydb2.cityobject, citydb2.cityobject_genericattrib
            WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'h_maaiveld'
            ORDER BY bag_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id
        '''
    )

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET h_dak_70p = subquery.h_dak_70p
        FROM
            (SELECT cityobject.gmlid as bag_id, cityobject_genericattrib.realval AS h_dak_70p
            FROM citydb2.cityobject, citydb2.cityobject_genericattrib
            WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'h_dak_70p'
            ORDER BY bag_id) AS subquery
        WHERE training_data.{table}_tmp.bag_id = subquery.bag_id
        '''
    )
    return

def get_height_values_3DBM(cursor, table, lod):
    #get max_Z, min_Z, ground_Z again
    print(f'\n>> Dataset {table} -- obtaining {lod} height values from 3DBM')

    #Add new column to format bag id <- REMINDER: THIS MAKE IT DOES NOT TAKE BUILDINGS WITH UNDERGROUND PARTS INTO ACCOUNT
    cursor.execute(f"ALTER TABLE input_data.{lod}_3dbm ADD COLUMN IF NOT EXISTS new_id VARCHAR")

    cursor.execute(f'''
        UPDATE input_data.{lod}_3dbm
        SET new_id = LEFT(id, -2)
        '''
    )

    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS max_z_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS min_z_{lod} DOUBLE PRECISION;")
    cursor.execute(f"ALTER TABLE training_data.{table}_tmp ADD COLUMN IF NOT EXISTS ground_z_{lod} DOUBLE PRECISION;")

    cursor.execute(f'''
        UPDATE training_data.{table}_tmp
        SET 
        max_z_{lod} = {lod}_3dbm."max_Z_{lod}",
        min_z_{lod} = {lod}_3dbm."min_Z_{lod}",    
        ground_z_{lod} = {lod}_3dbm."ground_Z_{lod}"
        FROM input_data.{lod}_3dbm
        WHERE training_data.{table}_tmp.bag_id = input_data.{lod}_3dbm.new_id;
        '''
    )

    cursor.execute(f'''
        ALTER TABLE input_data.{lod}_3dbm DROP COLUMN new_id;
        '''
    )
    return

def main():
    with open('params.json', 'r') as f:
        params = json.load(f)
        
        buffer_size = params['buffer_size']
        neighbour_distances = params['neighbour_distances']

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #create a cursor
    cursor = conn.cursor()

    table = 'validate_rh'
    #create temporary validation table of all buildings to store data used for validation of the features
    create_temp_validation_table(cursor, table, pkey='bag_id')

    #adjacency
    validate_no_adjacent_bldg(cursor, table, buffer_size)

    #neighbours
    validate_no_neighbours(cursor, table, neighbour_distances)

    #volumes
    validate_volumes(cursor, table)

    table = 'validate_rh2'
    #create ANOTHER temporary validation table of all buildings to store data used for validation of the features
    create_temp_validation_table(cursor, table, pkey='bag_id')

    #obb
    validate_obb(cursor, table)

    #surface areas
    validate_surface_areas(cursor, table)

    #height
    validate_height_values(cursor, table)

    return

if __name__ == '__main__':
    main()