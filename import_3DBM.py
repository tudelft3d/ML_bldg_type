import json
import pandas as pd
import db_functions
from sqlalchemy import create_engine

def prepare_3DBM_features(lod):

    with open('params.json', 'r') as f:
        params = json.load(f)

        path_3DBAG = params['path_3DBAG']

    print(f'\n>> Preparing {lod} 3DBM features from {path_3DBAG}merged_{lod}.csv')
    
    #Read the LoD 1.2 or 2.2 .csv files
    data = pd.read_csv(path_3DBAG+f'merged_{lod}.csv')

    #id and 3D Building Metrics to keep as features
    #max_Z and min_Z of roof
    features = ['id','actual_volume','convex_hull_volume',
                'footprint_perimeter','obb_width','obb_length',
                'ground_area', 'wall_area', 'roof_area', 'ground_point_count',
                'max_Z', 'min_Z', 'ground_Z',
                'shared_walls_area', 'closest_distance']
    data_features = data[features]
    
    #Assign LoD to columnnames
    new_columnnames = ['id']
    for i in range(len(features)-1):
        new_columnnames.append(features[i+1] + f'_{lod}')
    
    data_features.columns = new_columnnames

    return data_features

def import_3DBM_features(cursor, conn1, lod):

    #Prepare the metrics from 3DBM to be imported as features
    features = prepare_3DBM_features(lod)

    print(f'\n>> Importing {lod} 3DBM features to database input_data.{lod}_3dbm')

    #Create table to store 3DBM features
    cursor.execute(
        f"DROP TABLE IF EXISTS input_data.{lod}_3dbm; " +
        f"CREATE TABLE input_data.{lod}_3dbm (id VARCHAR, actual_volume_{lod} DOUBLE PRECISION, convex_hull_volume_{lod} DOUBLE PRECISION, " +
        f"footprint_perimeter_{lod} DOUBLE PRECISION, obb_width_{lod} DOUBLE PRECISION, obb_length_{lod} DOUBLE PRECISION, " +
        f"ground_area_{lod} DOUBLE PRECISION, wall_area_{lod} DOUBLE PRECISION, roof_area_{lod} DOUBLE PRECISION, ground_point_count_{lod} INTEGER, " +
        f"max_Z_{lod} DOUBLE PRECISION, min_Z_{lod} DOUBLE PRECISION, ground_Z_{lod} DOUBLE PRECISION, " +
        f"shared_walls_area_{lod} DOUBLE PRECISION, closest_distance_{lod} DOUBLE PRECISION);"
    )

    #Import 3DBM features
    features.to_sql(f'{lod}_3dbm', conn1, schema='input_data', if_exists= 'replace')

    return

def main():
    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    #import connection to db
    db = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    conn1 = db.connect()

    #create a cursor
    cursor = conn.cursor()

    lod1 = 'lod1'
    lod2 = 'lod2'
    # lod1_3DBM_features = prepare_3DBM_features(lod1)
    # lod2_3DBM_features = prepare_3DBM_features(lod2)

    import_3DBM_features(cursor, conn1, lod1)
    import_3DBM_features(cursor, conn1, lod2)

    #close db connections
    db_functions.close_connection(conn, cursor)
    conn1.commit()
    conn1.close()
    return

if __name__ == '__main__':
    main()