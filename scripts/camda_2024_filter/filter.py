import pandas as pd
import json
import os
import sys
import re

ROOT:str = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir
    )
)
sys.path.append(ROOT)

with open('00_origin_data/daae_generated_diabetes_patients_camda_gen2.json', 'r') as f:
	origin = json.load(f)

data = origin.copy()
print(f'N: {len(data.keys())}')


pattern = re.compile(r'^90[0-9]{2}$')
age_count = 0
p_id_duplicate_age = []
duplicate_age = 0
for k, obj in data.items():
	flag=False
	for obj_list in obj:
		age_count = 0
		for visit in obj_list:
			if bool(pattern.search(visit)):
				age_count += 1

        if age_count == 1:
            flag = True
			
	if flag == False:
		p_id_duplicate_age.append(k)
		duplicate_age += 1

for id in p_id_duplicate_age:
	del data[id]
print(f'N: {len(data.keys())}, dropped {duplicate_age}')

no_dm = 0
p_id = []
for k, obj in data.items():
	flag=False
	for obj_list in obj:
		if '401' in obj_list:
			flag=True
	if flag == False:
		p_id.append(k)
		no_dm += 1


for id in p_id:
	del data[id]
print(f'N: {len(data.keys())}, dropped {no_dm}')

no_age = 0
p_id_no_age = []
pattern = re.compile(r'^90[0-9]{2}$')
for k, obj in data.items():
	flag=False
	for obj_list in obj:
		for visit in obj_list:
			if bool(pattern.search(visit)):
				flag=True
	if flag == False:
		p_id_no_age.append(k)
		no_age += 1

for id in p_id_no_age:
	del data[id]
print(f'N: {len(data.keys())}, dropped {no_age}')

