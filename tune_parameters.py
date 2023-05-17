import json
import db_functions
import pandas as pd
import select_features
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import LinearSVC, SVC
from sklearn import preprocessing
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold, validation_curve
import numpy as np
from matplotlib import pyplot

def cross_validation(X_train, y_train, algorithm, param_name, param_range):
    scoring = 'accuracy'
    ylabel = 'Accuracy'
    if algorithm == 'rf':
        estimator = RandomForestClassifier(random_state=0)
    elif algorithm == 'svc':
        estimator = LinearSVC(random_state=0)
    else:
        print('Unknown algorithm')
        return
    
    scaler = preprocessing.RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_train_scaled_df = pd.DataFrame(X_train_scaled, columns = X_train.columns)

    kfold = StratifiedKFold(n_splits=3, shuffle=True, random_state=0)

    train_scores, test_scores = validation_curve(estimator, X_train_scaled_df, y_train, param_name=param_name, param_range=param_range, cv=kfold, n_jobs=-1, verbose=2, scoring=scoring)

    train_scores_mean = np.nanmean(train_scores, axis=1)
    test_scores_mean = np.nanmean(test_scores, axis=1)

    pyplot.xlabel(param_name)
    pyplot.ylabel(ylabel)
    pyplot.plot(param_range, train_scores_mean, label='Training score')
    pyplot.plot(param_range, test_scores_mean, label='CV Test score')
    pyplot.legend(loc='best')
    pyplot.show()
    return

def best_params(X_train, y_train, algorithm):
    if algorithm == 'rf':
        with open('params.json', 'r') as f:
            params = json.load(f)

            n_estimators = params['rf_n_estimators']
            max_depth = params['rf_max_depth']
            min_samples_split = params['rf_min_samples_split']
            min_samples_leaf = params['rf_min_samples_leaf']
            max_features = params['rf_max_features']

        criterion = ['gini', 'entropy', 'log_loss']
        bootstrap = [True, False]
        class_weight = ['balanced', 'balanced_subsample', None]

        cv_grid = {'n_estimators': n_estimators,
                   'criterion': criterion,
                   'max_depth': max_depth,
                   'min_samples_split': min_samples_split,
                   'min_samples_leaf': min_samples_leaf,
                   'max_features': max_features,
                   'bootstrap': bootstrap,
                   'class_weight': class_weight}
        
        estimator = RandomForestClassifier(random_state=0)
        
    elif algorithm == 'svc':
        with open('params.json', 'r') as f:
            params = json.load(f)

            tol = params['tol']
            C = params['C']
            max_iter = params['max_iter']

        loss = ['hinge','squared_hinge']
        dual = [True, False]
        class_weight = [None, 'balanced']

        cv_grid = {'loss': loss,
                   'dual': dual,
                   'tol': tol,
                   'C': C,
                   'class_weight': class_weight,
                   'max_iter': max_iter}
        
        estimator = LinearSVC(random_state=0)

    else:
        print('Unknown algorithm')
        return

    kfold = StratifiedKFold(n_splits=3, shuffle=True, random_state=0)

    cv_random = RandomizedSearchCV(estimator=estimator, param_distributions=cv_grid, n_iter=75, cv=kfold, verbose=2, random_state=0, n_jobs=-1, error_score=0.0, scoring='balanced_accuracy')

    scaler = preprocessing.RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_train_scaled_df = pd.DataFrame(X_train_scaled, columns = X_train.columns)

    search = cv_random.fit(X_train_scaled_df, y_train)
    print('\nBest hyperparameters of best estimator: ', search.best_estimator_.get_params())
    print('\nBest score: ', search.best_score_)
    print('\nBest hyperparameters of search obj: ', search.best_params_)

    return search.best_estimator_

def main():
    with open('params.json', 'r') as f:
        params = json.load(f)
        
        table = params['table']

        n_estimators = params['rf_n_estimators']
        max_depth = params['rf_max_depth']
        min_samples_split = params['rf_min_samples_split']
        min_samples_leaf = params['rf_min_samples_leaf']
        max_features = params['rf_max_features']

        tol = params['tol']
        C = params['C']
        max_iter = params['max_iter']

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

    #SVC
    # -anova-f features
    X_train, X_test, y_train, y_test = select_features.split_data(anova_features)
    cross_validation(X_train, y_train, 'svc', 'tol', tol)
    cross_validation(X_train, y_train, 'svc', 'C', C)
    cross_validation(X_train, y_train, 'svc', 'max_iter', max_iter)

    # -mi features
    X_train, X_test, y_train, y_test = select_features.split_data(mi_features)
    cross_validation(X_train, y_train, 'svc', 'tol', tol)
    cross_validation(X_train, y_train, 'svc', 'C', C)
    cross_validation(X_train, y_train, 'svc', 'max_iter', max_iter)

    #RANDOM FOREST
    # -impurity features
    X_train, X_test, y_train, y_test = select_features.split_data(impurity_features)
    cross_validation(X_train, y_train, 'rf', 'n_estimators', n_estimators)
    cross_validation(X_train, y_train, 'rf', 'max_depth', max_depth)
    cross_validation(X_train, y_train, 'rf', 'min_samples_split', min_samples_split)
    cross_validation(X_train, y_train, 'rf', 'min_samples_leaf', min_samples_leaf)
    cross_validation(X_train, y_train, 'rf', 'max_features', max_features)


    # -permutation features
    X_train, X_test, y_train, y_test = select_features.split_data(permutation_features)
    cross_validation(X_train, y_train, 'rf', 'n_estimators', n_estimators)
    cross_validation(X_train, y_train, 'rf', 'max_depth', max_depth)
    cross_validation(X_train, y_train, 'rf', 'min_samples_split', min_samples_split)
    cross_validation(X_train, y_train, 'rf', 'min_samples_leaf', min_samples_leaf)
    cross_validation(X_train, y_train, 'rf', 'max_features', max_features)
    return

if __name__ == '__main__':
    main()