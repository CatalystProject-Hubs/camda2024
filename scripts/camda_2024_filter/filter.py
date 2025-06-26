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

#DATABASE:str = 'daae_generated_diabetes_patients_camda_gen2'
DATABASE:str = 'diabetia_daae'

# Reading data
data:dict = {} 
#with open(f'{ROOT}/00_origin_data/daae_generated_diabetes_patients_camda_gen2.json', 'r') as f:
with open(f'{ROOT}/00_origin_data/{DATABASE}.json', 'r') as f:
	data = json.load(f)

print(f'N: {len(data.keys())}')

# ============================================================================
# Drop patients with no DM2 (401) or with DM2 without age
# ============================================================================
no_dm = 0
p_id = []
pattern = re.compile(r'^9[0-9]{3}$')

for patient, record in data.items():
	flag_dm=False

	for visit in record:
		if '401' in visit:
			flag_dm=True
			break

	if flag_dm == False:
		p_id.append(patient)
		no_dm += 1

for id in p_id:
	del data[id]

print(f'N: {len(data.keys())}, dropped {no_dm} || Patients without DM2')

# ============================================================================
# Drop patients with D2M without age
# ============================================================================
no_dm_age = 0
p_id_no_dm_age = []
pattern = re.compile(r'^9[0-9]{3}$')

for patient, record in data.items():
	flag_dm_age=False

	for visit in record:
		if '401' in visit and any([bool(pattern.match(code)) for code in visit]):
			flag_dm_age=True
			break

	if flag_dm_age == False:
		p_id_no_dm_age.append(patient)
		no_dm_age += 1

for id in p_id_no_dm_age:
	del data[id]

print(f'N: {len(data.keys())}, dropped {no_dm_age} || Patients without age at DM2 diagnosis')

# ============================================================================
# Clean sex variable
# ============================================================================
sex_clean = 0

for patient, record in data.items():
	records = record.copy()
	
	# impute sex
	if not any(['1111' in visit or '2222' in visit for visit in record]):
		record.insert(0,['3333'])
		continue
	
	sex_visits = [visit for visit in record if ('1111' in visit or '2222' in visit)]
	if len(sex_visits) > 1:
		for s_visit in sex_visits[1:]:
			if '1111' in s_visit:
				record[record.index(s_visit)].remove('1111')
				record[record.index(sex_visits[0])].append('1111')
			
			if '2222' in s_visit:
				record[record.index(s_visit)].remove('2222')
				record[record.index(sex_visits[0])].append('2222')

	
	for visit in sex_visits[:1]:
		
		# expected sex visit
		if ('1111' in visit or '2222' in visit) and len(visit)==1:
			continue
		
		# duplicated sex
		if ('1111' in visit and '2222' in visit) and len(visit)==2:
			record.remove(visit)
			record.insert(0,['3333'])
		else:
			other_codes_index = record.index(visit)
			record.remove(visit)
			sexs = list(set([code for code in visit if code in ['1111','2222']]))
			if len(sexs) > 1:		
				sexs = ['3333']
			
			other_codes = [code for code in visit if code not in ['1111','2222']]
			record.insert(other_codes_index,other_codes)
			record.insert(0,sexs)
			
		
#print(f'N: {len(data.keys())}, dropped {no_age} || Patients without at least 1 age in some of their visits')

# ============================================================================
# Drop patients with only 1 visit
# ============================================================================
"""
In this process next steps are considered:

	-> Visits with no Dx or no Age are dropped. Then, remain valid 
	visits are consider to drop those patients with only 1 valid visit. 
	Example:
		* [401, 901] = NOT VALID
		* [9045, 9023] = NOT VALID
		* [9075] = NOT VALID
		* [401] = NOT VALID
		* [401, 9045] = VALID
		* [401, 9045, 9034] = VALID

	-> Visits in one of the next 4 targets are not dropped even if they do 
	not have age at dx but are not considered as visit:
		* 703: Retinopatía
		* 910: Cardiopatía isquémica
		* 1999: Amputacion
		* 1401: Insuficiencia renal crónica

	-> ALL patients must have sex code. If some patient does not have
	sex code, '3333' will be added instead.

	-> One visit is considered when both age and dx are present in list
"""

patients = list(data.keys())
p_id_one_visit = []
one_visit = 0
pattern = re.compile(r'^9[0-9]{3}$')
dx_targets = ['703','910','1999','1401']

for patient in patients:
	record = data[patient]
	record_origin = record.copy()

	for visit in record_origin:
		
		# validate for target dx
		if any([target in visit for target in dx_targets]):
			if not any([bool(pattern.match(code)) for code in visit]):
				record[record.index(visit)].append('9999')
			continue 
		
		# empty or 1 size visits are dropped
		if len(visit)<=1:
			if '1111' in visit or '2222' in visit:
				continue
			else:
				record.remove(visit)
				continue
		
		# drop visits with no age or with no dx
		ages_in_visit = [int(v[0:]) for v in visit if bool(pattern.match(v)) == True]
		
		if len(visit) <= len(ages_in_visit):
			record.remove(visit)
			continue
        
		if len(ages_in_visit) == 0:
			record.remove(visit)
			continue

	valid_visits = [visit for visit in record if '9999' not in visit]
	
	if len(valid_visits) <= 2:
		p_id_one_visit.append(patient)
		one_visit+=1
		del data[patient]
	
print(f'N: {len(data.keys())}, dropped {one_visit} || Patients with only 1 visit')

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
pattern = re.compile(r'^9[0-9]{3}$')

for patient, record in data.items():
	# initialize age_list by patient
	dm_age = None
	age_list = []

	for visit in record:
		if any(code in visit for code in ['1111','2222','3333','9999']):
			continue
		
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

if not os.path.exists(f'{ROOT}/00_origin_data/{DATABASE}'):
	os.mkdir(f'{ROOT}/00_origin_data/{DATABASE}')

if not os.path.exists(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/'):
    os.mkdir(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/filtered_data.json', 'w', encoding='utf-8') as file:
	json.dump(data, file, ensure_ascii=False, indent=4)
	print(f'>>> filtered database saved: {len(data.keys())}')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/future.json', 'w', encoding='utf-8') as file:
	json.dump(future | complete, file, ensure_ascii=False, indent=4)
	print(f'>>> future database saved: {len(future.keys())+len(complete.keys())}')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/complete.json', 'w', encoding='utf-8') as file:
	json.dump(complete, file, ensure_ascii=False, indent=4)
	print(f'>>> complete database saved: {len(complete.keys())}')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/past.json', 'w', encoding='utf-8') as file:
	json.dump(complete | past, file, ensure_ascii=False, indent=4)
	print(f'>>> past database saved: {len(complete.keys()) + len(past.keys())}')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/only_future.json', 'w', encoding='utf-8') as file:
	json.dump(future, file, ensure_ascii=False, indent=4)
	print(f'>>> only future database saved: {len(future.keys())}')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/only_past.json', 'w', encoding='utf-8') as file:
	json.dump(past, file, ensure_ascii=False, indent=4)
	print(f'>>> only past database saved: {len(past.keys())}')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/one_year.json', 'w', encoding='utf-8') as file:
	json.dump(one_year, file, ensure_ascii=False, indent=4)
	print(f'>>> one_year database saved: {len(one_year.keys())}')

with open(f'{ROOT}/00_origin_data/{DATABASE}/db_filtered/errors.json', 'w', encoding='utf-8') as file:
    json.dump(errors, file, ensure_ascii=False, indent=4)







	
			
		




