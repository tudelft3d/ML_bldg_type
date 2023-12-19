# Queries used for pre-processing of the EP-online ground truth
The following queries for pre-processing were applied to Case Study 2. However, these queries can be applied to the other case studies as well by replacing `c2_delft` with the other case study tables (for example, `c3_flat`, `c3_galerij` etc.)

## Buildings with a list of building types
Show the buildings with a list of building types:
```sql
SELECT bag_id, building_type2, building_type, no_adjacent_bldg, no_adjacent_of_adja_bldg, bag_no_dwellings
FROM training_data.c2_delft
WHERE building_type IS NOT NULL
AND cardinality(building_type) != 1
ORDER BY bag_id
```

Create a temporary table to count the number of occurrences of each building type in the list for each building:
```sql
CREATE TABLE training_data.c2_delft_tmp AS
SELECT bag_id, building_type2,
	cardinality(array_positions(building_type, 'Appartement'))
    AS appartement,
	cardinality(array_positions(building_type, 'Maisonnette'))
    AS maisonnette,
	cardinality(array_positions(building_type, 'Galerijwoning'))
    AS galerijwoning,
	cardinality(array_positions(building_type, 'Portiekwoning'))
    AS portiekwoning,
	cardinality(array_positions(building_type, 'Flatwoning (overig)'))
    AS flatwoning,
	building_type
FROM training_data.c2_delft
WHERE building_type IS NOT NULL
AND cardinality(building_type) != 1
ORDER BY bag_id
```

Update the building type of each building with the highest occurrence building type:
```sql
UPDATE training_data.c2_delft
SET building_type2 = a.newbuilding_type
FROM (SELECT bag_id,
	  cardinality(array_positions(building_type, 'Appartement'))
      AS appartement,
	  cardinality(array_positions(building_type, 'Maisonnette'))
      AS maisonnette,
	  cardinality(array_positions(building_type, 'Galerijwoning'))
      AS galerijwoning,
	  cardinality(array_positions(building_type, 'Portiekwoning'))
      AS portiekwoning,
	  cardinality(array_positions(building_type, 'Flatwoning (overig)'))
      AS flatwoning,
	  CASE
	  	WHEN appartement > maisonnette
            AND appartement > galerijwoning
            AND appartement > portiekwoning
	  	    AND appartement > flatwoning
            THEN 'Flatwoning (overig)'
	  	WHEN maisonnette >= appartement
            AND maisonnette > galerijwoning
            AND maisonnette > portiekwoning
	  	    AND maisonnette >= flatwoning
            THEN 'Maisonette'
	  	WHEN galerijwoning >= appartement
            AND galerijwoning > maisonnette
            AND galerijwoning > portiekwoning
	  	    AND galerijwoning >= flatwoning
            THEN 'Galerijwoning'
	  	WHEN portiekwoning >= appartement
            AND portiekwoning > maisonnette
            AND portiekwoning > galerijwoning
	  		AND portiekwoning >= flatwoning
            THEN 'Portiekwoning'
	  	WHEN flatwoning >= appartement
            AND flatwoning > maisonnette
            AND flatwoning > galerijwoning
	  		AND flatwoning > portiekwoning
            THEN 'Flatwoning (overig)'
	  	WHEN appartement = 0
            AND maisonnette = 0
            AND galerijwoning = 0
            AND portiekwoning = 0
	  		AND flatwoning = 0
            THEN NULL
	  END AS newbuilding_type
	FROM training_data.c2_delft_tmp) a
WHERE building_type IS NOT NULL
AND cardinality(building_type) != 1
AND training_data.c2_delft.bag_id = a.bag_id;
```

## Buildings with building type 'Semi-detached or End house' as one entry
Show the buildings with one entry in the list of building types, but that one entry has the building type 'semi-detached or end house':
```sql
SELECT bag_id, building_type, building_type2, no_adjacent_bldg, no_adjacent_of_adja_bldg, bag_no_dwellings
FROM training_data.c2_delft, unnest(building_type) AS building_type3
WHERE building_type IS NOT NULL
AND cardinality(building_type) = 1
AND building_type3 LIKE '%/%'
ORDER BY bag_id
```

Assign the 'semi-detached' (Twee-onder-één-kap) building type to these buildings:
```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Twee-onder-één-kap'
FROM (SELECT * 
	 FROM training_data.c2_delft, unnest(building_type) AS building_type3) a
WHERE c2_delft.building_type IS NOT NULL
AND cardinality(c2_delft.building_type) = 1
AND a.building_type3 LIKE '%/%'
AND c2_delft.no_adjacent_bldg = 1
AND c2_delft.no_adjacent_of_adja_bldg = 1
AND c2_delft.bag_no_dwellings = 1
AND c2_delft.bag_id = a.bag_id;
```

Assign the 'end house' (Rijwoning hoek) building type to these buildings:
```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Rijwoning hoek'
FROM (SELECT * 
	 FROM training_data.c2_delft, unnest(building_type) AS building_type3) a
WHERE c2_delft.building_type IS NOT NULL
AND cardinality(c2_delft.building_type) = 1
AND a.building_type3 LIKE '%/%'
AND c2_delft.no_adjacent_bldg = 1
AND c2_delft.no_adjacent_of_adja_bldg > 1
AND c2_delft.bag_no_dwellings = 1
AND c2_delft.bag_id = a.bag_id;
```

Assign the 'terraced house' (Rijwoning tussen) building type to these buildings:
```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Rijwoning tussen'
FROM (SELECT * 
	 FROM training_data.c2_delft, unnest(building_type) AS building_type3) a
WHERE c2_delft.building_type IS NOT NULL
AND cardinality(c2_delft.building_type) = 1
AND a.building_type3 LIKE '%/%'
AND c2_delft.no_adjacent_bldg > 1
AND c2_delft.bag_no_dwellings = 1
AND c2_delft.bag_id = a.bag_id;
```

## Buildings with one building type in the list
Show the buildings with one building type in the list:
```sql
SELECT bag_id, building_type, building_type2, no_adjacent_bldg, no_adjacent_of_adja_bldg, bag_no_dwellings
FROM training_data.c2_delft, unnest(building_type) AS building_type3
WHERE building_type IS NOT NULL
AND cardinality(building_type) = 1
AND building_type3 NOT LIKE '%/%'
```

Assign buildings with one building type with that building type:
```sql
UPDATE training_data.c2_delft
SET building_type2 = a.building_type3
FROM (SELECT *
	  FROM training_data.c2_delft, unnest(building_type) AS building_type3) a
WHERE c2_delft.building_type IS NOT NULL
AND cardinality(c2_delft.building_type) = 1
AND a.building_type3 NOT LIKE '%/%'
AND c2_delft.bag_id = a.bag_id;
```

## Buildings with 'Appartement' or 'NULL' as building type
Show the buildings with 'Appartment' as building type:
```sql
SELECT bag_id, building_type, building_type2, no_adjacent_bldg, no_adjacent_of_adja_bldg, bag_no_dwellings
FROM training_data.c2_delft, unnest(building_type) AS building_type3
WHERE building_type IS NOT NULL
AND cardinality(building_type) = 1
AND building_type3 NOT LIKE '%/%'
AND building_type3 = 'Appartement'
ORDER BY bag_id
```

Show the buildings with 'NULL' as building type:
```sql
SELECT bag_id, building_type, building_type2, no_adjacent_bldg, no_adjacent_of_adja_bldg, bag_no_dwellings
FROM training_data.c2_delft, unnest(building_type) AS building_type3
WHERE building_type IS NOT NULL
AND cardinality(building_type) = 1
AND building_type3 IS NULL
ORDER BY bag_id
```

Assign the buildings with 'NULL' as building type with building type 'detached house' (Vrijstaande woning):
```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Vrijstaande Woning'
WHERE building_type2 IS NULL
AND no_adjacent_bldg = 0
AND bag_no_dwellings = 1;

```

The building types of the other buildings with 'Appartement' or 'NULL' as building type had to be manually assigned to their building type. This was done by visual inspection through their BAG IDs. The following query assigns their building type (replace `[building type]` with the appropriate building type and `[condition]` with the condition to filter out a building (for example, `bag_id= ...`)):

```sql
UPDATE training_data.c2_delft
SET building_type2 = [building type]
WHERE [condition];
```

## Clean up the building type names
```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Flatwoning'
WHERE building_type2 = 'Flatwoning (overig)';
```

```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Hoekwoning'
WHERE building_type2 = 'Rijwoning hoek';
```

```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Maisonnettewoning'
WHERE building_type2 = 'Maisonette' OR building_type2 = 'Maisonnette';
```

```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Tussenwoning'
WHERE building_type2 = 'Rijwoning tussen'
```

```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Twee-onder-een-kapwoning'
WHERE building_type2 = 'Twee-onder-één-kap';
```

```sql
UPDATE training_data.c2_delft
SET building_type2 = 'Vrijstaande Woning'
WHERE building_type2 = 'Vrijstaande woning';
```

## Clean up the building type columns
Query to remove old building type column (list of building types):
```sql
ALTER TABLE training_data.c2_delft
DROP COLUMN building_type;
```


Query to rename (new) building type column (building_type2) to main building type column (building_type):
```sql
ALTER TABLE training_data.c2_delft 
RENAME COLUMN building_type2 TO building_type;
```