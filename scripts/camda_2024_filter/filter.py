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


# ============================================================================
# Drop patients with no DM2 (401)
# ============================================================================
no_dm = 0
p_id = []
for patient, record in data.items():
	flag_dm=False

	for visit in record:
		if '401' in visit:
			flag_dm=True

	if flag_dm == False:
		p_id.append(patient)
		no_dm += 1

for id in p_id:
	del data[id]

print(f'N: {len(data.keys())}, dropped {no_dm}')

# ============================================================================
# Drop patients with no age
# ============================================================================
no_age = 0
p_id_no_age = []
pattern = re.compile(r'^90[0-9]{2}$')

for patient, record in data.items():
	flag_age=False
	
	for visit in record:
		for code in visit:
			if bool(pattern.search(code)):
				flag_age=True
				break
		
		if flag_age:
			break

	if flag_age == False:
		p_id_no_age.append(patient)
		no_age += 1

for id in p_id_no_age:
	del data[id]

print(f'N: {len(data.keys())}, dropped {no_age}')

# ============================================================================
# Drop patients with only 1 visit
# ============================================================================
"""
In this process visits with no Dx or no Age are dropped. Then, remain valid 
visits are consider to drop those patients with only 1 valid visit. Example:
	[401, 901] = NOT VALID
	[9045, 9023] = NOT VALID
	[9075] = NOT VALID
	[401] = NOT VALID
	[401, 9045] = VALID
	[401, 9045, 9034] = VALID
"""

patients = list(data.keys())
p_id_one_visit = []
one_visit = 0
pattern = re.compile(r'^90[0-9]{2}$')

for patient in patients:
	record = data[patient]
	record_origin = record.copy()

	for visit in record_origin:
		
		# empty or 1 size visits are dropped
		if len(visit)<=1:
			record.remove(visit)
			continue
		
		# drop visits with no age or with no dx
		ages_in_visit = [int(v[1:]) for v in visit if bool(pattern.match(v)) == True]
		
		if len(visit) <= len(ages_in_visit):
			record.remove(visit)
			continue
        
		if len(ages_in_visit) == 0:
			record.remove(visit)
			continue
	
	if len(record)<=1:
		p_id_one_visit.append(patient)
		one_visit+=1
		del data[patient]
	
print(f'N: {len(data.keys())}, dropped {one_visit}')

# ============================================================================
# Divide patients in different datasets
# ============================================================================
future = {}
past = {}
complete = {}
one_year = {}
errors = {}
one_year = {}
dm = '401'
pattern = re.compile(r'^90[0-9]{2}$')

for patient, record in data.items():
	# initialize age_list by patient
	dm_age = None
	age_list = []

	for visit in record:
		# identify dm age
		if dm in visit:
			dm_ages = [int(v[1:]) for v in visit if bool(pattern.match(v)) == True]
            
			if len(dm_ages) != 0:
				if dm_age != None:
					dm_age = min(dm_ages+[dm_age])
				else:
					dm_age = min(dm_ages)

		ages_in_visit = [int(v[1:]) for v in visit if bool(pattern.match(v)) == True]
		min_age_in_visit = min(ages_in_visit)
		age_list.append(min_age_in_visit)
	
	# Drop cases where D2M was None
	if dm_age == None:
		errors[patient] = record
		continue
	
	# identify boundaries
	max_age = max(age_list)
	min_age = min(age_list)

	if max_age > dm_age:
		if min_age < dm_age:
			complete[patient] = record
		elif min_age == dm_age:
			future[patient] = record
	elif max_age == dm_age:
		if min_age < dm_age:
			past[patient] = record
		elif min_age == dm_age:
			one_year[patient] = record

if not os.path.exists(f'{ROOT}/00_origin_data/db_filtered/'):
    os.mkdir(f'{ROOT}/00_origin_data/db_filtered/')

with open(f'{ROOT}/00_origin_data/db_filtered/future.json', 'w', encoding='utf-8') as file:
	json.dump(future | complete, file, ensure_ascii=False, indent=4)
	print(f'>>> future database saved: {len(future.keys())+len(complete.keys())}')

with open(f'{ROOT}/00_origin_data/db_filtered/complete.json', 'w', encoding='utf-8') as file:
	json.dump(complete, file, ensure_ascii=False, indent=4)
	print(f'>>> complete database saved: {len(complete.keys())}')

with open(f'{ROOT}/00_origin_data/db_filtered/past.json', 'w', encoding='utf-8') as file:
	json.dump(complete | past, file, ensure_ascii=False, indent=4)
	print(f'>>> past database saved: {len(complete.keys()) + len(past.keys())}')

with open(f'{ROOT}/00_origin_data/db_filtered/only_future.json', 'w', encoding='utf-8') as file:
	json.dump(future, file, ensure_ascii=False, indent=4)
	print(f'>>> only future database saved: {len(future.keys())}')

with open(f'{ROOT}/00_origin_data/db_filtered/only_past.json', 'w', encoding='utf-8') as file:
	json.dump(past, file, ensure_ascii=False, indent=4)
	print(f'>>> only past database saved: {len(past.keys())}')

with open(f'{ROOT}/00_origin_data/db_filtered/one_visit.json', 'w', encoding='utf-8') as file:
	json.dump(one_year, file, ensure_ascii=False, indent=4)
	print(f'>>> one_year database saved: {len(one_year.keys())}')

with open(f'{ROOT}/00_origin_data/db_filtered/errors.json', 'w', encoding='utf-8') as file:
    json.dump(errors, file, ensure_ascii=False, indent=4)







	
			
		




