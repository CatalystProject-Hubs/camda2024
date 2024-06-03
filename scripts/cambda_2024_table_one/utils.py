import pandas as pd
import numpy as np

def ageRange(age:int)-> str:
    
    assert isinstance(age,(int,float)), 'Age must be an integer or float number'

    if pd.isnull(age) or age == None:
        age_cat = None

    age_cat = None
    if age < 18:
        age_cat = '0-18'
    elif age < 45:
        age_cat = '18-44'
    elif age < 65:
        age_cat = '45-64'
    elif age >= 65:
        age_cat = '60>'

    return age_cat


if __name__ == '__main__':
    print(ageRange(20))