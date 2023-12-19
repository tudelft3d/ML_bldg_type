# Queries used for feature validation

## Adjacency
For visualisation of the building footprints with their adjacency features and building functions:

```sql
SELECT bag_id, bag_function, no_adjacent_bldg, no_adjacent_of_adja_bldg, footprint_geom
FROM training_data.validate1_c1_rh_tmp
ORDER BY bag_id ASC
```

Example with building 'NL.IMBAG.Pand.1742100000094682':
```sql
SELECT a.bag_id, COUNT(*) AS no_adjacent, ARRAY_AGG(b.bag_id)
FROM training_data.validate_rh_tmp AS a
JOIN training_data.validate_rh_tmp AS b ON ST_INTERSECTS(ST_Buffer(a.footprint_geom, 0.1, 'join=mitre'), b.footprint_geom)
WHERE a.bag_id != b.bag_id
AND a.bag_function != 'Others' AND a.bag_function != 'Unknown' 
AND b.bag_function != 'Others' AND b.bag_function != 'Unknown'
AND a.bag_id = 'NL.IMBAG.Pand.1742100000094682'
GROUP BY a.bag_id
```
## Neighbours
Showing the neighbour feature at different distances for each building:
```sql
SELECT bag_id, no_neighbours_25m, no_neighbours_50m, no_neighbours_75m, no_neighbours_100m
FROM training_data.c1_rh
ORDER BY bag_id
```

Example with building 'NL.IMBAG.Pand.0150100000000149', visualising the building footprints at distance of 25m:
```sql
SELECT b.bag_id, b.footprint_geom
FROM training_data.validate_rh_tmp AS a
JOIN training_data.validate_rh_tmp AS b
ON ST_DWithin(ST_Centroid(a.footprint_geom), b.footprint_geom, 25)
WHERE a.bag_id != b.bag_id AND a.bag_id = 'NL.IMBAG.Pand.0150100000000149'
ORDER BY b.bag_id
```

## Volume
Comparison of the volume features with the PostGIS calculated volumes and the volumes extracted from the Rijssen-Holten energy testbed dataset:
```sql
SELECT validate_rh_tmp.bag_id, actual_volume_lod1_postgis, actual_volume_lod1, convex_hull_volume_lod1, actual_volume_lod2_rh, actual_volume_lod2, convex_hull_volume_lod2
FROM training_data.validate_rh_tmp, training_data.c1_rh
WHERE actual_volume_lod1_postgis IS NOT NULL AND validate_rh_tmp.bag_id = c1_rh.bag_id
ORDER BY bag_id ASC 
```

## Length and width
Comparison of the PostGIS calculated area with the area calculated by multiplying the oriented bounding box length with the width (area_check):
```sql
SELECT validate2_rh_tmp.bag_id, obb_width_lod1, obb_length_lod1, area_from_bbox, area_check, bbox, footprint_geom
FROM training_data.validate2_rh_tmp, training_data.c1_rh
WHERE validate2_rh_tmp.bag_id = c1_rh.bag_id
ORDER BY bag_id ASC 
```

## Surface areas
Comparison of the surface area features with PostGIS calculated surface areas and surface areas extracted from the Rijssen-Holten energy testbed dataset:

```sql
SELECT c1_rh.bag_id, wall_area_lod1, roof_area_lod1, roof_area_lod1_postgis, wall_area_lod2, wall_area_lod2_rh, roof_area_lod2, roof_area_lod2_rh
FROM training_data.c1_rh, training_data.validate2_rh_tmp
WHERE c1_rh.bag_id = validate2_rh_tmp.bag_id
ORDER BY c1_rh.bag_id
```

## Height
Comparison of the raw height values obtained from 3DBM with the corresponding 3D BAG height values:

```sql
SELECT bag_id,
h_dak_max, max_z_lod2,
h_dak_min, min_z_lod2,
h_maaiveld, ground_z_lod1, ground_z_lod2,
h_dak_70p, max_z_lod1, min_z_lod1
FROM training_data.validate2_rh_tmp
ORDER BY bag_id
```