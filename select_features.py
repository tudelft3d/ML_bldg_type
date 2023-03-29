import db_functions
import json
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import SimpleImputer, IterativeImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn_pandas import DataFrameMapper
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_classif
from sklearn.feature_selection import mutual_info_classif
from matplotlib import pyplot

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.svm import LinearSVC

def read_data(conn, table):

    print(f'\n>> Dataset {table} -- reading data into pandas dataframe')

    data = pd.read_sql_query(f"SELECT * FROM training_data.{table};", conn)

    # #for testing purposes
    # print(data.head())
    # print(data.shape)

    #Split input X (features) and output y (target)
    var_columns = [c for c in data.columns if c not in ('bag_id', 'building_type')]
    X = data.loc[:, var_columns]
    y = data.loc[:, 'building_type']

    #Split into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=0)

    return X_train, X_test, y_train, y_test

def preprocess_mapper(X_train):

    num_features = list(X_train.select_dtypes(exclude=['object']))
    cat_features = list(X_train.select_dtypes(include=['object']))

    cat = [([c], [SimpleImputer(strategy='most_frequent'), SimpleImputer(strategy='most_frequent', missing_values=None), OneHotEncoder(drop='if_binary')]) for c in cat_features]
    num = [([n], [IterativeImputer(random_state=0), StandardScaler()]) for n in num_features]
    mapper = DataFrameMapper(num + cat, df_out=True)
    X_train_prep = mapper.fit_transform(X_train)
    
    return X_train_prep

def select_kbest_features(X_train, y_train, X_test, score_func, k):
    fs = SelectKBest(score_func=score_func, k=k)

    #learn relationship from training data
    fs.fit(X_train, y_train)

    #get column names
    column_names = list(X_train.columns.values)
    #print(column_names)

    #what are the scores for the features
    print(f'\n>> Scores of each feature with SelectKBest using {score_func}:')
    for i in range(len(fs.scores_)):
        print(f'Feature {i}: {column_names[i]} = {fs.scores_[i]}')

    #plot the scores
    pyplot.bar([i for i in range(len(fs.scores_))], fs.scores_)
    pyplot.show()
    return

def compare_with_SVMs(X_train, y_train, X_test, y_test, score_func, k):
    #NOTE: for testing purposes LinearSVC() is used
    clf = make_pipeline(MinMaxScaler(), LinearSVC())
    clf.fit(X_train, y_train)
    print(f"Classification accuracy without selecting features: {clf.score(X_test, y_test)}")

    clf_selected = make_pipeline(SelectKBest(score_func=score_func, k=k), MinMaxScaler(), LinearSVC())
    clf_selected.fit(X_train, y_train)
    print(f"Classification accuracy after univariate feature selection: {clf_selected.score(X_test, y_test)}")
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

    #read dataframe and split into X (features) and y (target) and then into train and test split (80% - 20%)
    X_train, X_test, y_train, y_test = read_data(conn, table)

    #preprocess numerical and categorical features
    X_train_prep = preprocess_mapper(X_train)
    X_test_prep = preprocess_mapper(X_test)

    #k = number of features to be selected
    k = 10
    #Using SelectKBest to find the scores for each feature
    #ANOVA F-value between label/feature: variation between the variables / variation within the variable
    select_kbest_features(X_train_prep, y_train, X_test_prep, f_classif, k)
    #Mutual information for discrete target: measures the dependency between the variables
    select_kbest_features(X_train_prep, y_train, X_test_prep, mutual_info_classif, k)

    compare_with_SVMs(X_train_prep, y_train, X_test_prep, y_test, f_classif, k)
    compare_with_SVMs(X_train_prep, y_train, X_test_prep, y_test, mutual_info_classif, k)
    return

if __name__ == '__main__':
    main()