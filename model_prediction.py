import json
import db_functions
import pandas as pd
import select_features
import tune_parameters
from sklearn import preprocessing
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, balanced_accuracy_score
from time import time

def model_results(X_train, X_test, y_train, y_test, algorithm):
    starttime1 = time()
    if algorithm == 'rf':
        #get estimator with best hyperparameters
        estimator = tune_parameters.best_params(X_train, y_train, algorithm)
    
    elif algorithm == 'svc':
        estimator = tune_parameters.best_params(X_train, y_train, algorithm)

    else:
        print('Unknown algorithm')
        return
    endtime1 = time()
    duration1 = endtime1 - starttime1

    scaler = preprocessing.RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_train_scaled_df = pd.DataFrame(X_train_scaled, columns = X_train.columns)
    X_test_scaled = scaler.transform(X_test)

    starttime2 = time()
    estimator.fit(X_train_scaled_df, y_train)
    endtime2 = time()
    duration2 = endtime2 - starttime2

    print("Hyperparameter tuning time: ", round(duration1, 2), 's')
    print("Training time: ", round(duration2, 2), 's')

    y_pred = estimator.predict(X_test_scaled)
    result = confusion_matrix(y_test, y_pred)
    print("Confusion Matrix:")
    print(result)
    result1 = classification_report(y_test, y_pred)
    print("Classification report:")
    print(result1)
    result2 = accuracy_score(y_test, y_pred)
    print("Accuracy: ", result2)
    result3 = balanced_accuracy_score(y_test, y_pred)
    print("Balanced accuracy: ", result3)
    return estimator

def split_data_for_comparison(data):
    #Split input X (features) and output y (target)
    var_columns = [c for c in data.columns if c not in ('bag_id', 'building_type')]
    X = data.loc[:, var_columns]
    y = data.loc[:, 'building_type']
    return X, y

def model_results2(X_train, X, y, model):
    scaler = preprocessing.RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_scaled = scaler.transform(X)
    X_scaled_df = pd.DataFrame(X_scaled, columns = X.columns)

    print("Comparing:")
    y_pred = model.predict(X_scaled_df)
    result = confusion_matrix(y, y_pred)
    print("Confusion Matrix:")
    print(result)
    result1 = classification_report(y, y_pred)
    print("Classification report:")
    print(result1)
    result2 = accuracy_score(y, y_pred)
    print("Accuracy: ", result2)
    result3 = balanced_accuracy_score(y, y_pred)
    print("Balanced accuracy: ", result3)
    return

def main():
    with open('params.json', 'r') as f:
        params = json.load(f)
        
        table = params['table']

    #get db parameters
    user,password,database,host,port = db_functions.get_db_parameters()

    #create connection to db with the db parameters
    conn = db_functions.setup_connection(user,password,database,host,port)
    conn.autocommit = True

    if table == 'c1_rh':

        data = pd.read_sql_query(f"SELECT * FROM training_data.{table};", conn)

        #Remove features based on correlation
        data = data.drop(['actual_volume_lod1',
                        'convex_hull_volume_lod1',
                        'convex_hull_volume_lod2',
                        'wall_area_lod1',
                        'roof_area_lod1',
                        'height_max_lod1',
                        'no_storeys'], axis='columns')
        
        #Remove features based on feature selection methods
        anova_features = data.drop(['no_neighbours_25m',
                                    'roof_area_lod2',
                                    'fp_perimeter',
                                    'obb_length_lod1',
                                    'fp_no_vertices',
                                    'height_max_lod2',
                                    'height_min_roof_lod2',
                                    'bag_construction_year',
                                    'no_neighbours_75m',
                                    'no_neighbours_100m',
                                    'fp_area'], axis='columns')
        
        mi_features = data.drop(['fp_perimeter',
                                'roof_area_lod2',
                                'obb_length_lod1',
                                'wall_area_lod2',
                                'height_max_lod2',
                                'height_min_roof_lod2',
                                'bag_no_dwellings',
                                'fp_no_vertices',
                                'no_neighbours_75m',
                                'no_neighbours_25m',
                                'no_neighbours_100m'], axis='columns')
        
        impurity_features = data.drop(['roof_area_lod2',
                                    'wall_area_lod2',
                                    'fp_perimeter',
                                    'obb_length_lod1',
                                    'height_min_roof_lod2',
                                    'no_neighbours_50m',
                                    'fp_no_vertices',
                                    'bag_no_dwellings',
                                    'no_neighbours_75m',
                                    'no_neighbours_100m',
                                    'fp_area'], axis='columns')
        
        permutation_features = data.drop(['no_neighbours_100m',
                                    'actual_volume_lod2',
                                    'fp_perimeter',
                                    'no_neighbours_75m',
                                    'no_neighbours_50m',
                                    'obb_length_lod1',
                                    'fp_no_vertices',
                                    'fp_area',
                                    'height_min_roof_lod2',
                                    'roof_area_lod2',
                                    'height_max_lod2'], axis='columns')

        X_train, X_test, y_train, y_test = select_features.split_data(anova_features)
        anova_svc_model = model_results(X_train, X_test, y_train, y_test, 'svc')

        X_train, X_test, y_train, y_test = select_features.split_data(mi_features)
        mi_svc_model = model_results(X_train, X_test, y_train, y_test, 'svc')

        X_train, X_test, y_train, y_test = select_features.split_data(impurity_features)
        impurity_rf_model = model_results(X_train, X_test, y_train, y_test, 'rf')

        X_train, X_test, y_train, y_test = select_features.split_data(permutation_features)
        permutation_rf_model = model_results(X_train, X_test, y_train, y_test, 'rf')

    elif table != 'c1_rh':
        data = pd.read_sql_query(f"SELECT * FROM training_data.c1_rh;", conn)

        #Remove features based on correlation
        data = data.drop(['actual_volume_lod1',
                        'convex_hull_volume_lod1',
                        'convex_hull_volume_lod2',
                        'wall_area_lod1',
                        'roof_area_lod1',
                        'height_max_lod1',
                        'no_storeys'], axis='columns')
        
        #Remove features based on feature selection methods
        anova_features = data.drop(['no_neighbours_25m',
                                    'roof_area_lod2',
                                    'fp_perimeter',
                                    'obb_length_lod1',
                                    'fp_no_vertices',
                                    'height_max_lod2',
                                    'height_min_roof_lod2',
                                    'bag_construction_year',
                                    'no_neighbours_75m',
                                    'no_neighbours_100m',
                                    'fp_area'], axis='columns')
        
        mi_features = data.drop(['fp_perimeter',
                                'roof_area_lod2',
                                'obb_length_lod1',
                                'wall_area_lod2',
                                'height_max_lod2',
                                'height_min_roof_lod2',
                                'bag_no_dwellings',
                                'fp_no_vertices',
                                'no_neighbours_75m',
                                'no_neighbours_25m',
                                'no_neighbours_100m'], axis='columns')
        
        impurity_features = data.drop(['roof_area_lod2',
                                    'wall_area_lod2',
                                    'fp_perimeter',
                                    'obb_length_lod1',
                                    'height_min_roof_lod2',
                                    'no_neighbours_50m',
                                    'fp_no_vertices',
                                    'bag_no_dwellings',
                                    'no_neighbours_75m',
                                    'no_neighbours_100m',
                                    'fp_area'], axis='columns')
        
        permutation_features = data.drop(['no_neighbours_100m',
                                    'actual_volume_lod2',
                                    'fp_perimeter',
                                    'no_neighbours_75m',
                                    'no_neighbours_50m',
                                    'obb_length_lod1',
                                    'fp_no_vertices',
                                    'fp_area',
                                    'height_min_roof_lod2',
                                    'roof_area_lod2',
                                    'height_max_lod2'], axis='columns')
        
        data2 = pd.read_sql_query(f"SELECT * FROM training_data.{table};", conn)

        #Remove features based on correlation
        data2 = data2.drop(['actual_volume_lod1',
                      'convex_hull_volume_lod1',
                      'convex_hull_volume_lod2',
                      'wall_area_lod1',
                      'roof_area_lod1',
                      'height_max_lod1'], axis='columns')
    
        #Remove features based on feature selection methods
        anova_features2 = data2.drop(['no_neighbours_25m',
                                'roof_area_lod2',
                                'fp_perimeter',
                                'obb_length_lod1',
                                'fp_no_vertices',
                                'height_max_lod2',
                                'height_min_roof_lod2',
                                'bag_construction_year',
                                'no_neighbours_75m',
                                'no_neighbours_100m',
                                'fp_area'], axis='columns')
    
        mi_features2 = data2.drop(['fp_perimeter',
                             'roof_area_lod2',
                             'obb_length_lod1',
                             'wall_area_lod2',
                             'height_max_lod2',
                             'height_min_roof_lod2',
                             'bag_no_dwellings',
                             'fp_no_vertices',
                             'no_neighbours_75m',
                             'no_neighbours_25m',
                             'no_neighbours_100m'], axis='columns')
    
        impurity_features2 = data2.drop(['roof_area_lod2',
                                   'wall_area_lod2',
                                   'fp_perimeter',
                                   'obb_length_lod1',
                                   'height_min_roof_lod2',
                                   'no_neighbours_50m',
                                   'fp_no_vertices',
                                   'bag_no_dwellings',
                                   'no_neighbours_75m',
                                   'no_neighbours_100m',
                                   'fp_area'], axis='columns')
    
        permutation_features2 = data2.drop(['no_neighbours_100m',
                                  'actual_volume_lod2',
                                  'fp_perimeter',
                                  'no_neighbours_75m',
                                  'no_neighbours_50m',
                                  'obb_length_lod1',
                                  'fp_no_vertices',
                                  'fp_area',
                                  'height_min_roof_lod2',
                                  'roof_area_lod2',
                                  'height_max_lod2'], axis='columns')

        X_train, X_test, y_train, y_test = select_features.split_data(anova_features)
        anova_svc_model = model_results(X_train, X_test, y_train, y_test, 'svc')

        X, y = split_data_for_comparison(anova_features2)
        model_results2(X_train, X, y, anova_svc_model)

        # X_train, X_test, y_train, y_test = select_features.split_data(mi_features)
        # mi_svc_model = model_results(X_train, X_test, y_train, y_test, 'svc')

        # X, y = split_data_for_comparison(mi_features2)
        # model_results2(X_train, X, y, mi_svc_model)

        # X_train, X_test, y_train, y_test = select_features.split_data(impurity_features)
        # impurity_rf_model = model_results(X_train, X_test, y_train, y_test, 'rf')

        # X, y = split_data_for_comparison(impurity_features2)
        # model_results2(X_train, X, y, impurity_rf_model)

        # X_train, X_test, y_train, y_test = select_features.split_data(permutation_features)
        # permutation_rf_model = model_results(X_train, X_test, y_train, y_test, 'rf')

        # X, y = split_data_for_comparison(permutation_features2)
        # model_results2(X_train, X, y, permutation_rf_model)

    return

if __name__ == '__main__':
    main()