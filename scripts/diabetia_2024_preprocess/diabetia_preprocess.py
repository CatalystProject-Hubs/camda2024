import json
import polars as pl
import os
import re
import sys
import numpy as np
ROOT:str = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir
    )
)
sys.path.append(ROOT)


def identifyGroup(code:str=None, patterns:dict=None):
    group = None
    if patterns == None:
        patterns = PATTERNS

    for g,patt in patterns.items():
        pattern = re.compile(patt)
        if bool(pattern.match(code)):
            group = g
            return group

    return group

if __name__ == '__main__':

    # read data
    # ..diagnosis
    data = pl.read_csv(f'{ROOT}/00_origin_data/exprel_diag.csv', separator=',')
    
    # ..patients
    patients = pl.read_csv(f'{ROOT}/00_origin_data/filtered_diabetia_cx_curps.csv', separator=',')
    patients_age = pl.read_csv(f'{ROOT}/00_origin_data/pacientes.csv', separator=',')
    
    # ..catalogs
    dx = pl.read_csv(f'{ROOT}/00_origin_data/gen2_CIE10.csv', separator=',')


    # ============================================================================
    # Filter data by cambda CIE-10 codes
    # ============================================================================
    # First step: prepare catalog with target cie-10
    clean_codes = []
    cie_codes = dx['CIE-10'].unique()
    for cie in cie_codes:
        if '-' not in cie:
            cie_group = cie
            clean_codes.append((cie,cie_group))
        else:
            cie_group = cie
            start = cie.split('-')[0]
            end = cie.split('-')[1]
            zeros = True if start[1] == '0' else False
            
            letter = start[0]
            start = int(start[1:])
            end = int(end[1:])

            code_range = np.arange(start,end+1,1)

            if zeros:
                code_range = [f'0{code}' for code in code_range]

            code_range = [(f'{letter}{code}',cie_group) for code in code_range]
            clean_codes.extend(code_range)

    # create dataframe with all 3-char cie-10 codes    
    target_cie = pl.DataFrame(clean_codes, schema=['cie','cie_group'])
    target_cie = target_cie.with_columns(
        pl.col('cie').map_elements(lambda x: x.replace('.',''))
    )

    # merge with dx and add camda code (CODE_BPS)
    target_cie = target_cie.join(
        dx,
        left_on='cie_group',
        right_on='CIE-10',
        how='left'
    )

    # filter data by cie-10 codes
    PATTERNS = {cie:r'^'+cie+r'.*$' for cie in target_cie['cie'].unique()}
    """data = data.with_columns(
        pl.col('catalog_key').map_elements(lambda x: identifyGroup(x, PATTERNS)).alias('cie_group')
    )"""

    data = data.with_columns(pl.lit('Z9999').alias('cie'))
    for group, regex in PATTERNS.items():
        data = data.with_columns(
            pl.when(pl.col('catalog_key').str.contains(regex)).then(pl.lit(group))
            .otherwise(pl.col('cie'))
            .alias('cie')
        )
        print(f"> {group}: {data.filter(pl.col('cie')==group).shape[0]}")

    data = data.join(
        target_cie[['CODE_BPS','cie','cie_group']],
        left_on='cie',
        right_on='cie',
        how = 'inner'
    )
    
    # ============================================================================
    # Filer data by diabetIA patients
    # ============================================================================
    # Add age to patients 
    print(f"N: {patients['cx_curp_original'].n_unique()} || initial patients")
    patients = patients.join(
        patients_age,
        left_on = 'cx_curp_original',
        right_on = 'cx_curp',
        how = 'left'
    )
    patients = patients.drop(['cx_curp'])
    patients = patients.rename({"cx_curp_original":"cx_curp"})


    # Patients with at least one camda variable
    data = data.join(
        patients[['cx_curp','df_nacimiento','cs_sexo']],
        left_on = 'cx_curp',
        right_on = 'cx_curp',
        how = 'inner'
    )
    print(f"N: {data['cx_curp'].n_unique()} || patients with some camda diagnosis")
    
    # filtering patients with T2D
    patients_with_t2d = data.filter(pl.col('cie_group')=='E10-E14')['cx_curp'].to_list()
    data = data.filter(pl.col('cx_curp').is_in(patients_with_t2d))
    print(f"N: {data['cx_curp'].n_unique()} || patients with T2D")

    
    # ============================================================================
    # Formatting data
    # ============================================================================
    data = data.with_columns(
        pl.col('df_consulta').map_elements(lambda x: x.split(' ')[0]),
        pl.col('df_nacimiento').str.strptime(pl.Date, "%Y-%m-%d"),
    )

    data = data.with_columns(
         pl.when(pl.col('cs_sexo')=='M').then(pl.lit('1111'))
            .when(pl.col('cs_sexo')=='F').then(pl.lit('2222'))
            .otherwise(pl.col('cs_sexo'))
            .alias('cs_sexo')
    )

    data = data.with_columns(
        pl.col('df_consulta').str.strptime(pl.Date, "%Y-%m-%d")
    )

    data = data.with_columns(
        ((pl.col('df_consulta') - pl.col('df_nacimiento')).dt.total_days() / 365.25).alias('age')
    ) 
    
    data = data.with_columns(
        pl.col('age').floor().cast(pl.Int64).alias('age')
    ) 

    data = data[['cx_curp','CODE_BPS','cs_sexo','age']].unique()

    # ============================================================================
    # Data to json
    # ============================================================================
    ids = data['cx_curp'].unique().to_list()
    data_dict = {}
    print('> Building json file')
    for p_id in ids:
        
        dx_list = []
        aux_data = data.filter(pl.col('cx_curp')==p_id)
        p_sex = aux_data['cs_sexo'][0]
        dx_list.append([p_sex])
        
        for row in aux_data.iter_rows():
            dx_list.append([str(row[1]),f'{row[3]+9000}'])

        data_dict[p_id] = dx_list
    
    
    with open(f'{ROOT}/00_origin_data/diabetia_daae.json', 'w') as f:
        json.dump(data_dict, f)

    print(f'>>> diabetia_daae file saved. {len(data_dict.keys())} final patients')

