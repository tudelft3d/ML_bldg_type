# Inferring the residential building type from 3DBAG
Master Thesis from Hoi-Kang Poon (Chris)

## Functional Requirements
- This repository requires a PostGIS database that contains the required data:
    - 3DCityDB
    -  Features / Open data

Users are required to create the file **db_parameters.txt** at the root of this project. This file is required so you can add the parameters to connect to the corresponding database that contains the data to perform the calculations required for this project. For security reasons, no database connections are provided in the python code.

Add the current time, the scripts support only one database connection.