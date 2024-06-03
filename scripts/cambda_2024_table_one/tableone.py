import pandas as pd
import numpy as np
import re
import os
import sys
import json
#import pingouin as pg

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

def tableOne(
        data:pd.DataFrame,
        config:dict, 
        group:str = None, 
        ttest:bool = False, 
        chi2test:bool = False,
        precision:int = 2
    ) -> pd.DataFrame:

    if group not in data.columns:
        data['_group'] = 'Global'
    else:
        data['_group'] = data[group]

    # headers
    headers = data.groupby('_group').size().reset_index()
    headers['variable'] = 'N'
    headers = headers.pivot(columns='_group', index = 'variable', values = 0).reset_index()

    # for numerical
    tbl_numerical = pd.DataFrame()
    for col in config['numerical']:
        aux = pd.DataFrame()
        aux = data.groupby('_group')[col].agg([np.mean,np.std]).reset_index()
        aux.insert(0,'variable', col)
        aux[col] = aux.apply(lambda x: f"{np.round(x['mean'],precision)} ({np.round(x['std'],precision)})", axis=1)
        aux.drop(columns = ['mean','std'], inplace=True)
        aux = aux.pivot(columns = '_group', index='variable', values=col).reset_index()

        tbl_numerical = pd.concat(
            [
                tbl_numerical,
                aux
            ],
            axis=0
        )

    # categorical
    tbl_categorical = pd.DataFrame()
    for col in config['categorical']:
        
        aux = pd.DataFrame()
        aux_percent = pd.DataFrame()

        aux = data[['_group',col]].value_counts().reset_index().rename(columns={0:'count'})
        aux_percent = (
            data[['_group',col]]
                .value_counts()/data[['_group']].value_counts()
            ).reset_index().rename(columns={0:'percent'})
        aux_percent['_group'] = aux_percent['_group'].apply(lambda x: f'{x} %')
        
        # ============================================================ PIVOT
        groups:list = aux['_group'].unique()
        
        # .... numerical
        aux = aux.pivot(columns = '_group', index=col, values = 'count').reset_index()
        if col == 'age_dm_cat' or col == 'age_cat':
            aux = aux.sort_values(by=aux.columns[0], ascending=True)    
        else:
            aux = aux.sort_values(by=aux.columns[1], ascending=False)

        # .... categorical
        aux_percent = aux_percent.pivot(columns = '_group', index=col, values = 'percent').reset_index()
        # ============================================================ 

        aux = (
            pd.merge(
                aux,
                aux_percent,
                on = col,
                how = 'left'
            )
            .rename(columns = {col:'variable'})
        )

        for g in groups:
            aux[g] = aux.apply(lambda x: f'{x[g]} ({np.round(x[g+" %"]*100,2):.2f}%)', axis=1)
            aux.drop(columns = g+' %', inplace=True)

        aux.replace(to_replace={'nan (nan%)':None}, inplace=True)
        if aux.shape[0] == 1:
            aux['variable'] =  col
        else:
            aux = pd.concat(
                [
                    pd.DataFrame({'variable':[col]}),
                    aux
                ],
                axis=0
            )

        tbl_categorical = pd.concat(
            [
                tbl_categorical,
                aux
            ],
            axis=0
        )
    
    tbl_one = pd.concat(
        [
            headers,    
            pd.concat(
                [tbl_numerical,tbl_categorical],axis=0
            )
        ]
    )
    return tbl_one



