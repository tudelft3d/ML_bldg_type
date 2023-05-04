# Inferring the residential building type from 3DBAG
This repository contains the code developed during the graduation project of Hoi-Kang Poon (Chris) for the MSc in Geomatics at TU Delft.

## Functional Requirements
This repository requires a PostGIS database containing:
- The [CityGML-Based 3D City model testbed for Energy-Related Applications](https://github.com/tudelft3d/Testbed4UBEM) (version 2022-11-07) in a [3D City database](https://www.3dcitydb.org/3dcitydb/) under the `citydb` schema.
- The [3D BAG](https://3dbag.nl) (v21.09.8) in the same 3D City database under the `citydb2` schema.
- The [Dutch National Energylabel dataset](https://www.ep-online.nl/) (v20230101_v2) imported as csv file into the `input_data` schema.
- The [BAG](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract) (retrieved on January 8 2023) dataset imported with `ogr2ogr` into the `input_data` schema.

Also:
- A folder with a subset of [3D BAG](https://3dbag.nl) (v21.09.8), in this case the tiles containing Rijssen-Holten are already provided (path needs to be defined in `params.json` file).

This implementation might work with different versions of the required datasets, but the results might be different.

Users are required to create the file **db_parameters.txt** at the root of this project. This file is required so you can add the parameters to connect to the corresponding database that contains the data to perform the calculations required for this project. For security reasons, no database connections are provided in the python code.

The **db_parameters.txt** should be structured as follows:
```
username
password
database
host
port
```

The conda environment can be recreated using the `environment.yml` file with the following command: `conda env create -f environment.yml`. Note: for [3DBM](https://github.com/tudelft3d/3d-building-metrics) to work older versions of `PyVista' and 'Shapely' are needed.

- PyVista v0.36.1
- Shapely 1.8.5

The implementation also requires the repository of [3DBM](https://github.com/tudelft3d/3d-building-metrics) (path needs to be defined in `params.json` file) and it requires [cjio](https://github.com/cityjson/cjio) to be installed.

## Usage
- `python utilize_3DBM.py` to merge the 3D BAG JSON files and filter LoD 1.2 and 2.2 with `cjio`, compute metrics of both LoDs with 3DBM and store the results in `merged_lod1.csv` and `merged_lod2.csv`. The results for this script are included in this repository, since running the script takes about 20 hours!
- `python import_3DBM.py` to keep only the relevant features from the results and import them to the PostGIS database in the `input_data` schema.
- `python import_groundtruth.py` to extract the labeled data from the [Dutch National Energylabel dataset](https://www.ep-online.nl/) and the [CityGML-Based 3D City model testbed for Energy-Related Applications](https://github.com/tudelft3d/Testbed4UBEM) to the `training_data` schema.
- `python extract_features.py` to extract features from the [BAG](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract) dataset and [3D BAG](https://3dbag.nl) dataset to the `training_data`.
- `python validate_features.py` to generate tables to validate certain extracted features, for example, the ones directly extracted from 3DBM.
- `python analyze_features.py` computes data statistics and create figures visualizing the extracted features for further analysis. 
- `python select_features.py` to perform feature selection with the filter and embedded method.
- `python tune_parameters.py` to plot validation curves for the hyperparameters and it contains the best_params() function to get the best hyperparameters.
- `python model_prediction.py` to make predictions and compute evaluation metrics via a confusion matrix.

`db_functions.py` contains database functions, like connecting/disconnecting, creating a temporary table etc.

## Parameters
The `params.json` contains the following parameters that needs be set by the user:
- `path_3DBAG`: path to a subset of 3DBAG containing the tiles for the specific case study.
- `path_3DBM`: path to 3DBM repository.
- `table`: table containing the specific case study.
- `buffer_size`: buffer size of the footprints for the computation of the adjacency feature.
It also contains the hyperparameters for Random Forest and SVC, the validation curves plotted in `tune_parameters.py` may help in defining the range of these hyperparameters.

At the current time, the scripts support only one database connection.
