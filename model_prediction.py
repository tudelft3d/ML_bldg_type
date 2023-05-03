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

    data = pd.read_sql_query(f"SELECT * FROM training_data.{table};", conn)

        #Remove features based on correlation
    data = data.drop(['actual_volume_lod1',
                      'convex_hull_volume_lod1',
                      'convex_hull_volume_lod2',
                      'wall_area_lod1',
                      'roof_area_lod1',
                      'height_max_lod1'], axis='columns')
    
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
                             'bag_construction_year',
                             'obb_length_lod1',
                             'wall_area_lod2',
                             'height_max_lod2',
                             'height_min_roof_lod2',
                             'bag_no_dwellings',
                             'fp_no_vertices',
                             'no_neighbours_75m',
                             'no_neighbours_25m',
                             'no_neighbours_100m'], axis='columns')
    
    impurity_features = data.drop(['height_max_lod2',
                                   'wall_area_lod2',
                                   'fp_perimeter',
                                   'obb_length_lod1',
                                   'height_min_roof_lod2',
                                   'no_neighbours_25m',
                                   'fp_no_vertices',
                                   'bag_no_dwellings',
                                   'no_neighbours_75m',
                                   'no_neighbours_100m',
                                   'fp_area'], axis='columns')
    
    permutation_features = data.drop(['no_neighbours_100m',
                                  'wall_area_lod2',
                                  'fp_perimeter',
                                  'no_neighbours_25m',
                                  'no_neighbours_50m',
                                  'obb_length_lod1',
                                  'fp_no_vertices',
                                  'fp_area',
                                  'height_min_roof_lod2',
                                  'roof_area_lod2',
                                  'bag_construction_year'], axis='columns')

    X_train, X_test, y_train, y_test = select_features.split_data(anova_features)
    model_results(X_train, X_test, y_train, y_test, 'svc')

    X_train, X_test, y_train, y_test = select_features.split_data(mi_features)
    model_results(X_train, X_test, y_train, y_test, 'svc')

    X_train, X_test, y_train, y_test = select_features.split_data(impurity_features)
    model_results(X_train, X_test, y_train, y_test, 'rf')

    X_train, X_test, y_train, y_test = select_features.split_data(permutation_features)
    model_results(X_train, X_test, y_train, y_test, 'rf')
    return

if __name__ == '__main__':
    main()