# Inferring the residential building type from 3DBAG
This repository contains the code developed during the graduation project of Hoi-Kang Poon (Chris) for the MSc in Geomatics at TU Delft.

## Functional Requirements
This repository requires:
- A 3D City database containing the [CityGML-Based 3D City model testbed for Energy-Related Applications](https://github.com/tudelft3d/Testbed4UBEM)
- A PostGIS database containing the [Dutch National Energylabel dataset](https://www.ep-online.nl/) and the [BAG](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract)

Users are required to create the file **db_parameters.txt** at the root of this project. This file is required so you can add the parameters to connect to the corresponding database that contains the data to perform the calculations required for this project. For security reasons, no database connections are provided in the python code.

The **db_parameters.txt** should be structured as follows:
```
username
password
database
host
port
```

At the current time, the scripts support only one database connection.
