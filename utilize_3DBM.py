'''
Utilizes cjio and 3DBM to first merge all tiles from case study
and then filter it into LoD 1.2 and 2.2. Lastly, the 3D Building
Metrics are computed and stored into .csv files.
'''

import json
import os

with open('params.json', 'r') as f:
    params = json.load(f)

    path_3DBAG = params['path_3DBAG']
    path_3DBM = params['path_3DBM']

#Uncomment to remove previously created files
if os.path.exists(f"{path_3DBAG}merged.city.json"):
  print(f"\n>> {path_3DBAG}merged.city.json already exists, removing...")
  os.remove(f"{path_3DBAG}merged.city.json")
if os.path.exists(f"{path_3DBAG}merged_lod1.city.json"):
  print(f"\n>> {path_3DBAG}merged_lod1.city.json already exists, removing...")
  os.remove(f"{path_3DBAG}merged_lod1.city.json".format(path_3DBAG))
if os.path.exists(f"{path_3DBAG}merged_lod2.city.json".format(path_3DBAG)):
  print(f"\n>> {path_3DBAG}merged_lod2.city.json already exists, removing...")
  os.remove(f"{path_3DBAG}merged_lod2.city.json")

#use cjio to merge all .json files into one and filter the LoD 1.2 and 2.2
print(f"\n>> Merging all .json files in {path_3DBAG} into merged.city.json using cjio:")
os.system(f"cjio empty.city.json merge '{path_3DBAG}*.json' save {path_3DBAG}merged.city.json")

print("\n>> Splitting LoD 1.2 of merged.city.json using cjio:")
os.system(f"cjio {path_3DBAG}merged.city.json lod_filter 1.2 save {path_3DBAG}merged_lod1.city.json")

print("\n>> Splitting LoD 2.2 of merged.city.json using cjio:")
os.system(f"cjio {path_3DBAG}merged.city.json lod_filter 2.2 save {path_3DBAG}merged_lod2.city.json")

#Compute metrics in LoD 1.2 and LoD 2.2 from merged .json file
print("\n>> Computing 3D Building Metrics from merged_lod1.city.json:")
os.system(f"python {path_3DBM}CityStats.py {path_3DBAG}merged_lod1.city.json -o {path_3DBAG}merged_lod1.csv")

print("\n>> Computing 3D Building Metrics from merged_lod2.city.json:")
os.system(f"python {path_3DBM}CityStats.py {path_3DBAG}merged_lod2.city.json -o {path_3DBAG}merged_lod2.csv")