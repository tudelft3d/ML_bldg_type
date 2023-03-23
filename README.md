# Inferring the residential building type from 3DBAG
This repository contains the code developed during the graduation project of Hoi-Kang Poon (Chris) for the MSc in Geomatics at TU Delft.

## Functional Requirements
This repository requires a PostGIS database containing:
- The [CityGML-Based 3D City model testbed for Energy-Related Applications](https://github.com/tudelft3d/Testbed4UBEM) in a [3D City database](https://www.3dcitydb.org/3dcitydb/).
- The [Dutch National Energylabel dataset](https://www.ep-online.nl/) imported as csv file into the `input_data` schema.
- The [BAG](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract) dataset imported with `ogr2ogr` into the `input_data` schema.

Also:
- A folder with a subset of [3D BAG](https://3dbag.nl), in this case the tiles containing Rijssen-Holten (path needs to be defined in `params.json` file).

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

`db_functions.py` contains database functions, like connecting/disconnecting, creating a temporary table etc.

At the current time, the scripts support only one database connection.
