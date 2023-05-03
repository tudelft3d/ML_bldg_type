import db_functions
import json
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
import sys
from sklearn.feature_selection import SelectKBest, f_classif, mutual_info_classif
from matplotlib import pyplot
from sklearn.pipeline import make_pipeline
from sklearn.svm import LinearSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.inspection import permutation_importance

def split_data(data):
    #Split input X (features) and output y (target)
    var_columns = [c for c in data.columns if c not in ('bag_id', 'building_type')]
    X = data.loc[:, var_columns]
    y = data.loc[:, 'building_type']

    #Split into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=0, stratify=y)

    return X_train, X_test, y_train, y_test

def select_kbest_features(table, X_train, y_train, score, k):
    #get column names
    column_names = list(X_train.columns.values)

    if score == 'anova_f':
        score_func = f_classif

    elif score == 'mutual_info':
        score_func = mutual_info_classif

    else:
        print('Unknown function')
        return

    fs = SelectKBest(score_func=score_func, k=k)
    fsp = make_pipeline(preprocessing.RobustScaler(), fs)
    fsp.fit(X_train, y_train)

    #write to csv file
    original_stdout = sys.stdout

    with open(f'results/{table}_score_{score}.csv', 'w') as f:
        sys.stdout = f    
        print(f'feature,name,score')
        for i in range(len(fs.scores_)):
            print(f'{i},{column_names[i]},{fs.scores_[i]}')
        sys.stdout = original_stdout

    #plot the scores
    pyplot.bar([i for i in range(len(fs.scores_))], fs.scores_)
    pyplot.show()
    return

def compare_with_SVMs(table, X_train, y_train, X_test, y_test, score, k):
    if score == 'anova_f':
        score_func = f_classif

    elif score == 'mutual_info':
        score_func = mutual_info_classif

    else:
        print('Unknown function')
        return

    #write to csv file
    original_stdout = sys.stdout

    with open(f'results/{table}_results_{score}_selection.csv', 'w') as f:
        sys.stdout = f

        clf = make_pipeline(preprocessing.RobustScaler(), LinearSVC(random_state=0, dual=False, max_iter=1000))
        clf.fit(X_train, y_train)
        print(f"Classification accuracy without selecting features,{clf.score(X_test, y_test)}")

        clf_selected = make_pipeline(preprocessing.RobustScaler(), SelectKBest(score_func=score_func, k=k), LinearSVC(random_state=0, dual=False, max_iter=1000))
        clf_selected.fit(X_train, y_train)
        print(f"Classification accuracy after {score} feature selection,{clf_selected.score(X_test, y_test)}")
        
        sys.stdout = original_stdout
    return

def randomforest_test(table, X_train, y_train, X_test, y_test):
    #get column names
    column_names = list(X_train.columns.values)

    rf = RandomForestClassifier(random_state=0)
    rfp = make_pipeline(preprocessing.RobustScaler(), rf)
    rfp.fit(X_train, y_train)

    #write to csv file
    original_stdout = sys.stdout

    with open(f'results/{table}_score_rf.csv', 'w') as f:
        sys.stdout = f    
        print(f'feature,name,score')
        for i in range(len(rf.feature_importances_)):
            print(f'{i},{column_names[i]},{rf.feature_importances_[i]}')
        sys.stdout = original_stdout

    #plot the scores
    pyplot.bar([i for i in range(len(rf.feature_importances_))], rf.feature_importances_)
    pyplot.show()

    scaler = preprocessing.RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    #test for overfitting
    print(f"Train Accuracy: {rfp.score(X_train, y_train)}")
    print(f"Test Accuracy: {rfp.score(X_test, y_test)}")

    #permutation based feature importance instead of impurity based (since its strongly biased and favor high cardinality features)
    result = permutation_importance(rf, X_test_scaled, y_test, n_repeats=5, random_state=0)

    sorted_importances_idx = result.importances_mean.argsort()
    importances = pd.DataFrame(result.importances[sorted_importances_idx].T, columns=X_train.columns[sorted_importances_idx],)

    ax = importances.plot.box(vert=False, whis=10)
    ax.set_title("Permutation Importances (train set)")
    ax.axvline(x=0, color="k", linestyle="--")
    ax.set_xlabel("Decrease in accuracy score")
    pyplot.show()
    return

def svc_overfitting_test(X_train, y_train, X_test, y_test):
    svc = LinearSVC(random_state=0, dual=False, max_iter=1000)
    svcp = make_pipeline(preprocessing.RobustScaler(),svc)
    svcp.fit(X_train, y_train)

    print(f"Train Accuracy: {svcp.score(X_train, y_train)}")
    print(f"Test Accuracy: {svcp.score(X_test, y_test)}")
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

    #split into X (features) and y (target) and then into train and test split (80% - 20%)
    X_train, X_test, y_train, y_test = split_data(data)

    #k = number of features to be selected
    k = 10
    #Using SelectKBest to find the scores for each feature
    #ANOVA F-value between label/feature: variation between the variables / variation within the variable
    select_kbest_features(table, X_train, y_train, 'anova_f', k)
    #Mutual information for discrete target: measures the dependency between the variables
    select_kbest_features(table, X_train, y_train, 'mutual_info', k)

    compare_with_SVMs(table, X_train, y_train, X_test, y_test, 'anova_f', k)
    compare_with_SVMs(table, X_train, y_train, X_test, y_test, 'mutual_info', k)

    randomforest_test(table, X_train, y_train, X_test, y_test)
    #svc_overfitting_test(X_train, y_train, X_test, y_test)

if __name__ == '__main__':
    main()