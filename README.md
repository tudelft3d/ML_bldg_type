# Inferring the residential building type from 3DBAG
This repository contains the code developed during the graduation project of Hoi-Kang Poon (Chris) for the MSc in Geomatics at TU Delft.

## Functional Requirements
This repository requires a PostGIS database containing:
- The [CityGML-Based 3D City model test-bed for Energy-Related Applications](https://github.com/tudelft3d/test-bed4UBEM) (version 2022-11-07) in a [3D City database](https://www.3dcitydb.org/3dcitydb/) under the `citydb` schema.
- The [3D BAG](https://3dbag.nl) (v21.09.8) in the same 3D City database under the `citydb2` schema. More schema's can be added to store different subsets of the 3D BAG. 
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
The order of the scripts listed below is also the order of execution.
- `python utilize_3DBM.py` to merge the 3D BAG JSON files and filter LoD 1.2 and 2.2 with `cjio`, compute metrics of both LoDs with 3DBM and store the results in `merged_lod1.csv` and `merged_lod2.csv`. The results of this script for `c1_rh` are included in this repository, since running the script on the `c1_rh` files takes about 20 hours!
- `python import_3DBM.py` to keep only the relevant features from the results and import them to the PostGIS database in the `input_data` schema.
- `python import_groundtruth.py` to extract the labelled data from the [Dutch National Energylabel dataset](https://www.ep-online.nl/) and the [CityGML-Based 3D City model test-bed for Energy-Related Applications](https://github.com/tudelft3d/test-bed4UBEM) to the `training_data` schema.
- `python extract_features.py` to extract features from the [BAG](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract) dataset and [3D BAG](https://3dbag.nl) dataset to the `training_data`.
- `python validate_features.py` to generate tables to validate certain extracted features, for example, the ones directly extracted from 3DBM.
- `python analyze_features.py` computes data statistics and create figures visualizing the extracted features for further analysis.
- `python select_features.py` to perform feature selection with the filter and embedded method.
- `python tune_parameters.py` to plot validation curves for the hyperparameters and it contains the best_params() function to get the best hyperparameters.
- `python model_prediction.py` to make predictions and compute evaluation metrics via a confusion matrix.

`db_functions.py` contains database functions, like connecting/disconnecting, creating a temporary table etc.

## Parameters
The `params.json` contains the following parameters that needs be set by the user:
- `table`: table containing the specific case study to which features needs to be extracted (for all case studies),
  and to validate features (only `c1_rh`),
  and to analyze features, select features, tune parameters and apply models (`c1_rh` and `c2_delft`).
- `table2`: table containing the specific case study to use the trained models on.
- `citydbx`: 3D City DB schema containing the specific case study from which the ground truth needs to be obtained (citydb and citydb2 are reserved for c1_rh).
- `path_3DBAG`: path to the folder containing the tiles of the 3D BAG subset for the specific case study from which the 3DBM features needs to be extracted.
- `path_3DBM`: path to 3DBM repository.
- `buffer_size`: buffer size of the footprints for the computation of the adjacency feature.

It also contains the hyperparameters for Random Forest and SVC, the validation curves plotted in `tune_parameters.py` may help in defining the range of these hyperparameters.

## Replicating results
For replicating the results obtained for the graduation project the trained models are provided in this repository. As well as the resulting training data tables, these can be restored from the provided DUMP file.

At the current time, the scripts support only one database connection.
