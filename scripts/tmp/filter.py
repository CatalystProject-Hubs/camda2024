import json
import os
import numpy as np

ROOT:str = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir
    )
)


with open(f'{ROOT}/00_origin_data/daae_generated_diabetes_patients_camda_gen2.json', 'r') as f:
    data = json.load(f)

future = {}
past = {}
complete = {}
one_visit = {}
errors = {}

dm = '401'

patients = list(data.keys())
for patient in patients:
    record = data[patient]
    if patient=='22':
        print('')
        pass

    dm_age = 0
    age_list = []
    for visit in record.copy():
        if len(visit)<=1:
            record.remove(visit)
            continue
        
        if dm in visit:
            dm_ages = [int(v[1:]) for v in visit if '90' in v and len(v)==4]
            
            if len(dm_ages) != 0:
                if dm_age != 0:
                    dm_age = min(dm_ages+[dm_age])
                else:
                    dm_age = min(dm_ages)

        ages_in_visit = [ int(v[1:]) for v in visit if '90' in v and len(v)==4]
        
        if len(visit) <= len(ages_in_visit):
            record.remove(visit)
            continue
        
        if len(ages_in_visit) == 0:
            record.remove(visit)
            continue

        min_age_in_visit = min(ages_in_visit)
        age_list.append(min_age_in_visit)
    
    if len(age_list) == 0:
        errors[patient] = record
        del data[patient]
        continue

    max_age = max(age_list)
    min_age = min(age_list)

    if dm_age == 0:
        errors[patient] = record
        del data[patient]
        continue
    
    if max_age > dm_age:
        if min_age < dm_age:
            complete[patient] = record
        elif min_age == dm_age:
            future[patient] = record
    elif max_age == dm_age:
        if min_age < dm_age:
            past[patient] = record
        elif min_age == dm_age:
            one_visit[patient] = record


if not os.path.exists(f'{ROOT}/00_origin_data/db_filtered/'):
    os.mkdir(f'{ROOT}/00_origin_data/db_filtered/')

with open(f'{ROOT}/00_origin_data/db_filtered/future.json', 'w', encoding='utf-8') as file:
    json.dump(future | complete, file, ensure_ascii=False, indent=4)

with open(f'{ROOT}/00_origin_data/db_filtered/complete.json', 'w', encoding='utf-8') as file:
    json.dump(complete, file, ensure_ascii=False, indent=4)

with open(f'{ROOT}/00_origin_data/db_filtered/past.json', 'w', encoding='utf-8') as file:
    json.dump(complete | past, file, ensure_ascii=False, indent=4)

with open(f'{ROOT}/00_origin_data/db_filtered/only_future.json', 'w', encoding='utf-8') as file:
    json.dump(future, file, ensure_ascii=False, indent=4)

with open(f'{ROOT}/00_origin_data/db_filtered/only_past.json', 'w', encoding='utf-8') as file:
    json.dump(past, file, ensure_ascii=False, indent=4)

with open(f'{ROOT}/00_origin_data/db_filtered/errors.json', 'w', encoding='utf-8') as file:
    json.dump(errors, file, ensure_ascii=False, indent=4)

print('Ready')