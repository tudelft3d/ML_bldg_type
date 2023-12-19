import json
import db_functions
import pandas as pd
import select_features
import tune_parameters
from sklearn import preprocessing
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, balanced_accuracy_score
from time import time
import sys
from sklearn.model_selection import train_test_split
from joblib import dump, load
import os

def split_data(data, table):
    #Split input X (features) and output y (target)
    var_columns = [c for c in data.columns if c not in ('bag_id', 'building_type')]
    X = data.drop(['building_type'], axis='columns')
    y = data.loc[:, 'building_type']

    #Split into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=0, stratify=y)

    #write to csv file
    X_test.to_csv(f'results/labels/{table}_labels_aftersplit.csv')

    X_train = X_train.drop(['bag_id'], axis='columns')
    X_test = X_test.drop(['bag_id'], axis='columns')

    return X_train, X_test, y_train, y_test

def model_results(X_train, X_test, y_train, y_test, table, algorithm, features):
    starttime1 = time()
    if algorithm == 'rf':
        #get estimator with best hyperparameters
        estimator = tune_parameters.best_params(X_train, y_train, algorithm)
    
    elif algorithm == 'svc':
        estimator = tune_parameters.best_params(X_train, y_train, algorithm)

    else:
        print('\nUnknown algorithm')
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

    print("\nHyperparameter tuning time: ", round(duration1, 2), 's')
    print("Training time: ", round(duration2, 2), 's')

    y_pred = estimator.predict(X_test_scaled)
    result = confusion_matrix(y_test, y_pred)
    print("\nConfusion Matrix:")
    print(result)
    result1 = classification_report(y_test, y_pred)
    print("\nClassification report:")
    print(result1)
    result2 = accuracy_score(y_test, y_pred)
    print("\nAccuracy: ", result2)
    result3 = balanced_accuracy_score(y_test, y_pred)
    print("\nBalanced accuracy: ", result3)

    #write to csv file
    original_stdout = sys.stdout

    with open(f'results/predictions/{table}_predictions_{algorithm}_{features}.csv', 'w') as f:
        sys.stdout = f
        print('row,prediction,correct_class, correct_prediction')
        for row_index, (input, prediction, label) in enumerate(zip(X_test.iterrows(), y_pred, y_test)):
            if prediction != label:
                print(f'{row_index},{prediction},{label}, FALSE')
            elif prediction == label:
                print(f'{row_index},{prediction},{label}, TRUE')
        sys.stdout = original_stdout

    return estimator

def split_data_for_comparison(data):
    #Split input X (features) and output y (target)
    var_columns = [c for c in data.columns if c not in ('bag_id', 'building_type')]
    X = data.loc[:, var_columns]
    y = data.loc[:, 'building_type']
    return X, y

def model_results2(X_train, X, y, model, table, table2, algorithm, features):
    scaler = preprocessing.RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_scaled = scaler.transform(X)
    X_scaled_df = pd.DataFrame(X_scaled, columns = X.columns)

    print("\nComparing:")
    y_pred = model.predict(X_scaled_df)
    result = confusion_matrix(y, y_pred)
    print("\nConfusion Matrix:")
    print(result)
    result1 = classification_report(y, y_pred)
    print("\nClassification report:")
    print(result1)
    result2 = accuracy_score(y, y_pred)
    print("\nAccuracy: ", result2)
    result3 = balanced_accuracy_score(y, y_pred)
    print("\nBalanced accuracy: ", result3)

    #write to csv file
    original_stdout = sys.stdout

    with open(f'results/predictions/{table}_predictions2_on_{table2}_{algorithm}_{features}.csv', 'w') as f:
        sys.stdout = f
        print('row,prediction,correct_class, correct_prediction')
        for row_index, (input, prediction, label) in enumerate(zip(X.iterrows(), y_pred, y)):
            if prediction != label:
                print(f'{row_index},{prediction},{label}, FALSE')
            elif prediction == label:
                print(f'{row_index},{prediction},{label}, TRUE')
        sys.stdout = original_stdout

    return

def main():
    with open('params.json', 'r') as f:
        params = json.load(f)
        
        table = params['table']
        table2 = params['table2']

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

        if os.path.exists(f'results/models/{table}_anova_svc_model.joblib'):
            print(f"\n{table}_anova_svc_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_anova_svc_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(anova_features, table)
            anova_svc_model = model_results(X_train, X_test, y_train, y_test, table, 'svc', 'anova_f')
            dump(anova_svc_model, f'results/models/{table}_anova_svc_model.joblib')

        if os.path.exists(f'results/models/{table}_mi_svc_model.joblib'):
            print(f"\n{table}_mi_svc_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_mi_svc_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(mi_features, table)
            mi_svc_model = model_results(X_train, X_test, y_train, y_test, table, 'svc', 'mi')
            dump(mi_svc_model, f'results/models/{table}_mi_svc_model.joblib')

        if os.path.exists(f'results/models/{table}_impurity_rf_model.joblib'):
            print(f"\n{table}_impurity_rf_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_impurity_rf_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(impurity_features, table)
            impurity_rf_model = model_results(X_train, X_test, y_train, y_test, table, 'rf', 'impurity')
            dump(impurity_rf_model, f'results/models/{table}_impurity_rf_model.joblib')

        if os.path.exists(f'results/models/{table}_permutation_rf_model.joblib'):
            print(f"\n{table}_permutation_rf_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_permutation_rf_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(permutation_features, table)
            permutation_rf_model = model_results(X_train, X_test, y_train, y_test, table, 'rf', 'permutation')
            dump(permutation_rf_model, f'results/models/{table}_permutation_rf_model.joblib')

        if table != table2:
            data2 = pd.read_sql_query(f"SELECT * FROM training_data.{table2};", conn)

            data2.to_csv(f'results/labels/{table}_{table2}_labels_for_comparison.csv')

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
            
            X_train, X_test, y_train, y_test = split_data(anova_features, table)
            X, y = split_data_for_comparison(anova_features2)
            anova_svc_model = load(f'results/models/{table}_anova_svc_model.joblib')
            model_results2(X_train, X, y, anova_svc_model, table, table2, 'svc', 'anova_f')

            X_train, X_test, y_train, y_test = split_data(mi_features, table)
            X, y = split_data_for_comparison(mi_features2)
            mi_svc_model = load(f'results/models/{table}_mi_svc_model.joblib')
            model_results2(X_train, X, y, mi_svc_model, table, table2, 'svc', 'mi')

            X_train, X_test, y_train, y_test = split_data(impurity_features, table)
            X, y = split_data_for_comparison(impurity_features2)
            impurity_rf_model = load(f'results/models/{table}_impurity_rf_model.joblib')
            model_results2(X_train, X, y, impurity_rf_model, table, table2, 'rf', 'impurity')

            X_train, X_test, y_train, y_test = split_data(permutation_features, table)
            X, y = split_data_for_comparison(permutation_features2)
            permutation_rf_model = load(f'results/models/{table}_permutation_rf_model.joblib')
            model_results2(X_train, X, y, permutation_rf_model, table, table2, 'rf', 'permutation')

    if table == 'c2_delft':
        data3 = pd.read_sql_query(f"SELECT * FROM training_data.{table};", conn)

        #Remove features based on correlation
        data3 = data3.drop(['actual_volume_lod1',
                        'convex_hull_volume_lod1',
                        'convex_hull_volume_lod2',
                        'wall_area_lod1',
                        'roof_area_lod1',
                        'height_max_lod1'], axis='columns')
    
        #Remove features based on feature selection methods
        anova_features3 = data3.drop(['actual_volume_lod2',
                                'no_adjacent_of_adja_bldg',
                                'roof_area_lod2',
                                'height_min_roof_lod2',
                                'fp_area',
                                'no_neighbours_50m',
                                'fp_no_vertices',
                                'no_neighbours_25m',
                                'no_neighbours_75m',
                                'no_neighbours_100m',
                                'bag_construction_year'], axis='columns')

        mi_features3 = data3.drop(['wall_area_lod2',
                                'height_max_lod2',
                                'no_adjacent_bldg',
                                'bag_construction_year',
                                'no_adjacent_of_adja_bldg',
                                'height_min_roof_lod2',
                                'fp_no_vertices',
                                'no_neighbours_25m',
                                'no_neighbours_50m',
                                'no_neighbours_100m',
                                'no_neighbours_75m'], axis='columns')

        impurity_features3 = data3.drop(['wall_area_lod2',
                                    'roof_area_lod2',
                                    'fp_perimeter',
                                    'obb_length_lod1',
                                    'height_min_roof_lod2',
                                    'bag_construction_year',
                                    'no_neighbours_100m',
                                    'no_neighbours_75m',
                                    'no_neighbours_50m',
                                    'fp_no_vertices',
                                    'no_neighbours_25m'], axis='columns')

        permutation_features3 = data3.drop(['bag_construction_year',
                                        'actual_volume_lod2',
                                        'height_min_roof_lod2',
                                        'height_max_lod2',
                                        'roof_area_lod2',
                                        'no_neighbours_25m',
                                        'no_neighbours_100m',
                                        'no_neighbours_50m',
                                        'fp_no_vertices',
                                        'fp_perimeter',
                                        'no_neighbours_75m'], axis='columns')

        if os.path.exists(f'results/models/{table}_anova_svc_model.joblib'):
            print(f"\n{table}_anova_svc_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_anova_svc_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(anova_features3, table)
            anova_svc_model2 = model_results(X_train, X_test, y_train, y_test, table, 'svc', 'anova_f')
            dump(anova_svc_model2, f'results/models/{table}_anova_svc_model.joblib')

        if os.path.exists(f'results/models/{table}_mi_svc_model.joblib'):
            print(f"\n{table}_mi_svc_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_mi_svc_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(mi_features3, table)
            mi_svc_model2 = model_results(X_train, X_test, y_train, y_test, table, 'svc', 'mi')
            dump(mi_svc_model2, f'results/models/{table}_mi_svc_model.joblib')

        if os.path.exists(f'results/models/{table}_impurity_rf_model.joblib'):
            print(f"\n{table}_impurity_rf_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_impurity_rf_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(impurity_features3, table)
            impurity_rf_model2 = model_results(X_train, X_test, y_train, y_test, table, 'rf', 'impurity')
            dump(impurity_rf_model2, f'results/models/{table}_impurity_rf_model.joblib')

        if os.path.exists(f'results/models/{table}_permutation_rf_model.joblib'):
            print(f"\n{table}_permutation_rf_model already exists. Delete or rename model to retrain and show test results.")

        else:
            print(f"\n{table}_permutation_rf_model does not exists. Hyperparameter tuning, train and show test results...")
            X_train, X_test, y_train, y_test = split_data(permutation_features3, table)
            permutation_rf_model2 = model_results(X_train, X_test, y_train, y_test, table, 'rf', 'permutation')
            dump(permutation_rf_model2, f'results/models/{table}_permutation_rf_model.joblib')

        if table != table2:
            data4 = pd.read_sql_query(f"SELECT * FROM training_data.{table2};", conn)

            data4.to_csv(f'results/labels/{table}_{table2}_labels_for_comparison.csv')

            #Remove features based on correlation
            if table2 == 'c1_rh':
                data4 = data4.drop(['actual_volume_lod1',
                            'convex_hull_volume_lod1',
                            'convex_hull_volume_lod2',
                            'wall_area_lod1',
                            'roof_area_lod1',
                            'height_max_lod1',
                            'no_storeys'], axis='columns')
            else:
                data4 = data4.drop(['actual_volume_lod1',
                            'convex_hull_volume_lod1',
                            'convex_hull_volume_lod2',
                            'wall_area_lod1',
                            'roof_area_lod1',
                            'height_max_lod1'], axis='columns')
        
            #Remove features based on feature selection methods
            anova_features4 = data4.drop(['actual_volume_lod2',
                                'no_adjacent_of_adja_bldg',
                                'roof_area_lod2',
                                'height_min_roof_lod2',
                                'fp_area',
                                'no_neighbours_50m',
                                'fp_no_vertices',
                                'no_neighbours_25m',
                                'no_neighbours_75m',
                                'no_neighbours_100m',
                                'bag_construction_year'], axis='columns')

            mi_features4 = data4.drop(['wall_area_lod2',
                                    'height_max_lod2',
                                    'no_adjacent_bldg',
                                    'bag_construction_year',
                                    'no_adjacent_of_adja_bldg',
                                    'height_min_roof_lod2',
                                    'fp_no_vertices',
                                    'no_neighbours_25m',
                                    'no_neighbours_50m',
                                    'no_neighbours_100m',
                                    'no_neighbours_75m'], axis='columns')

            impurity_features4 = data4.drop(['wall_area_lod2',
                                        'roof_area_lod2',
                                        'fp_perimeter',
                                        'obb_length_lod1',
                                        'height_min_roof_lod2',
                                        'bag_construction_year',
                                        'no_neighbours_100m',
                                        'no_neighbours_75m',
                                        'no_neighbours_50m',
                                        'fp_no_vertices',
                                        'no_neighbours_25m'], axis='columns')

            permutation_features4 = data4.drop(['bag_construction_year',
                                            'actual_volume_lod2',
                                            'height_min_roof_lod2',
                                            'height_max_lod2',
                                            'roof_area_lod2',
                                            'no_neighbours_25m',
                                            'no_neighbours_100m',
                                            'no_neighbours_50m',
                                            'fp_no_vertices',
                                            'fp_perimeter',
                                            'no_neighbours_75m'], axis='columns')
            
            X_train, X_test, y_train, y_test = select_features.split_data(anova_features3)
            X, y = split_data_for_comparison(anova_features4)
            anova_svc_model2 = load(f'results/models/{table}_anova_svc_model.joblib')
            model_results2(X_train, X, y, anova_svc_model2, table, table2, 'svc', 'anova_f')

            X_train, X_test, y_train, y_test = select_features.split_data(mi_features3)
            X, y = split_data_for_comparison(mi_features4)
            mi_svc_model2 = load(f'results/models/{table}_mi_svc_model.joblib')
            model_results2(X_train, X, y, mi_svc_model2, table, table2, 'svc', 'mi')

            X_train, X_test, y_train, y_test = select_features.split_data(impurity_features3)
            X, y = split_data_for_comparison(impurity_features4)
            impurity_rf_model2 = load(f'results/models/{table}_impurity_rf_model.joblib')
            model_results2(X_train, X, y, impurity_rf_model2, table, table2, 'rf', 'impurity')

            X_train, X_test, y_train, y_test = select_features.split_data(permutation_features3)
            X, y = split_data_for_comparison(permutation_features4)
            permutation_rf_model2 = load(f'results/models/{table}_permutation_rf_model.joblib')
            model_results2(X_train, X, y, permutation_rf_model2, table, table2, 'rf', 'permutation')

    return

if __name__ == '__main__':
    main()