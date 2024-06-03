import pandas as pd
import numpy as np
import re
import os
import sys
import json
#import pingouin as pg
from tableone import tableOne
from utils import ageRange

ROOT:str = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir
    )
)
sys.path.append(ROOT)

with open(f'{ROOT}/scripts/cambda_2024_table_one/config.json', 'r') as f:
    CONFIG:dict = json.load(f)

if __name__ == '__main__':
    
    # read data
    data = pd.read_csv(f'{ROOT}/00_origin_data/eHRs-gen2.csv')

    # summ data
    default_cols = ['p_id','sex']
    dx_cols = [
        "K73","H35","I65","J45","F30-F39","E66","I10-I15","E78","E10-E14","J44","I48","K21","M81","I20-I25","F09","F40-F41","I74","M15-M19","H40-H42","L40","I50","M10","F17","G45","F10","M13","F01-F03","N18","C18-C20","E03","N20","K90.4","I70","K76.0","G80-G83","M30-M31","C88","I34-I38","I60-I64","C81","G40-G41","C50","C82-C85","H35.3","I67-I69","F60-F69","K50-K51","C55","I69","F48","F70-F79","M05-M06","C22","K74","F50","F19","G25","C16","C61","I71","F20","Q20-Q28","L20","G20","C64-C65","M79.7","C67","C91-C95","C14","C25","F90-F98","C34","B20","F84","F89","C62","C40-C41","C53","Z89","C43","C46","C56"
    ]
    agg_dx = {
        k:'max' for k in dx_cols
    }
    agg_age = {
        'age':'min'
    }

    agg_cols = {
        **agg_dx,
        **agg_age
    }

    age_diab = (
        data.loc[data['E10-E14']==1]
            .sort_values(by=['p_id','age'], ascending=True)
            .drop_duplicates(subset=['p_id'],keep='first')
            [['p_id','age']]
            .rename(columns={'age':'age_dm'})
    )

    data = data.groupby(default_cols).agg(agg_cols).reset_index()
    data = pd.merge(
        data,
        age_diab,
        on = 'p_id',
        how = 'left'
    )

    data['age_dm_cat'] = data['age_dm'].apply(lambda x: ageRange(x))
    data['age_cat'] = data['age'].apply(lambda x: ageRange(x))
    data['diabetes'] = data['E10-E14'].apply(lambda x: 'DM' if x == 1 else 'No DM')
    
    # run tableone
    all_data = tableOne(data=data, config=CONFIG)
    by_group = tableOne(data=data, config=CONFIG, group='diabetes')

    final = pd.merge(
        all_data,
        by_group,
        on='variable',
        how='left'
    )
    final.to_csv(f'{ROOT}/00_origin_data/tbl_one.csv', index=False)

    print('ready')