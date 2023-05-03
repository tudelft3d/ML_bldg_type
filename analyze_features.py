'''
Data with statistics and visualization.
Based on: https://www.tutorialspoint.com/machine_learning_with_python/index.htm
'''

import db_functions
import json
import pandas as pd
from pandas import set_option
from matplotlib import pyplot
import numpy
from pandas.plotting import scatter_matrix
import os

def quick_datacheck(data):
    print('\nFirst 50 rows of the dataset:')
    print(data.head(50))
    print('\nTotal number of rows and columns:')
    print(data.shape)
    print('\nData types of each attribute:')
    print(data.dtypes)
    return

def stats_data(data, table):
    print('\nStatistical summary of the dataset:')
    set_option('display.width', 100)
    set_option('display.precision', 2)
    print(data.describe())

    #output to csv file
    os.makedirs('results', exist_ok=True)  
    data_out = data.describe()
    data_out.to_csv(f'results/{table}_data_statistics.csv')
    return

def class_distribution(data):
    print('\nClass distribution of the dataset:')
    count_class = data.groupby('building_type').size()
    print(count_class)
    return

def data_correlation(data, table):
    '''
    Coeff value = 1     - full positive correlation between variables
    Coeff value = -1    - full negative correlation between variables
    Coeff value = 0     - No correlation at all between variables
    '''
    print('\nCorrelation between Attributes:')
    set_option('display.width', 100)
    set_option('display.precision', 3)
    correlations = data.corr(method='pearson', numeric_only=True)
    print(correlations)

    # #output to csv file
    os.makedirs('results', exist_ok=True)  
    correlations.to_csv(f'results/{table}_data_corr.csv')
    return

def data_skew(data):
    '''
    If the value is closer to zero, then it shows less skew
    '''
    print('\nSkew of Attribute distribution:')
    print(data.skew(numeric_only=True))
    return

def data_histogram(data):
    '''
    Provides the count of observations in each bin
    From shape the distribution can be observed (Gaussian, skewed, exponential)
    Helps to spot possible outliers
    '''
    data.hist(layout=(4,6))
    pyplot.show()
    return

def data_densityplots(data):
    '''
    Similar to histograms but with a smooth curve
    '''
    dp = data.plot(kind='density', subplots=True, layout=(4,6), sharex=False)
    for subaxis in dp:
        for ax in subaxis:
            ax.legend(loc='upper left', fontsize=6, frameon=False)
    pyplot.show()
    return

def data_boxplots(data):
    '''
    Univariate and summarize distribution of each attribute
    Draws a line for the middle value (median)
    Draws a box around the 25% and 75%
    Draws whiskers about the spread of the data
    Dots outside whiskers signifies outlier values
    '''
    data.plot(kind='box', subplots=True, layout=(4,6), sharex=False, sharey=False)
    pyplot.show()
    return

def data_corrmatrix(data):
    correlations = data.corr()
    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    cax = ax.matshow(correlations, vmin=-1, vmax=1)
    fig.colorbar(cax)
    ticks = numpy.arange(0,24,1)
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    names = list(data.columns.values)
    ax.set_xticklabels(names[2:], rotation = 90)
    ax.set_yticklabels(names[2:])
    pyplot.show()
    return

def data_corrmatrix2(data):
    correlations = data.corr()
    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    cax = ax.matshow(correlations, vmin=-1, vmax=1)
    fig.colorbar(cax)
    ticks = numpy.arange(0,18,1)
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    names = list(data.columns.values)
    ax.set_xticklabels(names[2:], rotation = 90)
    ax.set_yticklabels(names[2:])
    pyplot.show()
    return

def data_scattermatrix(data):
    sm = scatter_matrix(data.rename(columns={'no_adjacent_bldg' : 1,
                                             'no_adjacent_of_adja_bldg' : 2,
                                             'no_neighbours_25m' : 3,
                                             'no_neighbours_50m' : 4,
                                             'no_neighbours_75m' : 5,
                                             'no_neighbours_100m' : 6,
                                             'bag_construction_year' : 7,
                                             'bag_no_dwellings' : 8,
                                             'fp_area' : 9,
                                             'fp_perimeter' : 10,
                                             'fp_no_vertices' : 11,
                                             'actual_volume_lod1' : 12,
                                             'convex_hull_volume_lod1' : 13,
                                             'obb_width_lod1' : 14,
                                             'obb_length_lod1' : 15,
                                             'wall_area_lod1' : 16,
                                             'roof_area_lod1' : 17,
                                             'height_max_lod1' : 18,
                                             'actual_volume_lod2' : 19,
                                             'convex_hull_volume_lod2' : 20,
                                             'wall_area_lod2' : 21,
                                             'roof_area_lod2' : 22,
                                             'height_max_lod2' : 23,
                                             'height_min_roof_lod2' : 24}))
    for subaxis in sm:
        for ax in subaxis:
            ax.xaxis.set_ticks([])
            ax.yaxis.set_ticks([])
    pyplot.show()
    return

def data_scattermatrix2(data):
    sm = scatter_matrix(data.rename(columns={'no_adjacent_bldg' : 1,
                                             'no_adjacent_of_adja_bldg' : 2,
                                             'no_neighbours_25m' : 3,
                                             'no_neighbours_50m' : 4,
                                             'no_neighbours_75m' : 5,
                                             'no_neighbours_100m' : 6,
                                             'bag_construction_year' : 7,
                                             'bag_no_dwellings' : 8,
                                             'fp_area' : 9,
                                             'fp_perimeter' : 10,
                                             'fp_no_vertices' : 11,
                                             'obb_width_lod1' : 12,
                                             'obb_length_lod1' : 13,
                                             'actual_volume_lod2' : 14,
                                             'wall_area_lod2' : 15,
                                             'roof_area_lod2' : 16,
                                             'height_max_lod2' : 17,
                                             'height_min_roof_lod2' : 18}))
    for subaxis in sm:
        for ax in subaxis:
            ax.xaxis.set_ticks([])
            ax.yaxis.set_ticks([])
    pyplot.show()
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

    # #Data statistics
    # # quick_datacheck(data)
    # # stats_data(data, table)
    # class_distribution(data)
    # data_correlation(data, table)
    # #visualision of correlation
    # data_corrmatrix(data)
    # data_skew(data)

    #Remove features based on correlation
    data = data.drop(['actual_volume_lod1',
                      'convex_hull_volume_lod1',
                      'convex_hull_volume_lod2',
                      'wall_area_lod1',
                      'roof_area_lod1',
                      'height_max_lod1'], axis='columns')
    data_correlation(data, table)
    #visualision of correlation
    data_corrmatrix2(data)

    #Additional Data visualization
    data_histogram(data)
    # data_densityplots(data)
    # data_boxplots(data)
    data_scattermatrix2(data)
    return

if __name__ == '__main__':
    main()