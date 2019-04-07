import pandas as pd
from pathlib import Path




cols = {
    'FeatureCode' : 'lsoa',
    'DateCode' : 'year',
    'Indices of Deprivation' : 'category',
    'value' : 'value',
    'Value' : 'value'
}

def transform_cols(old_cols):
    return [cols[x] for x in cols if x in cols]
    

conv_cat  = { 
    'b' : 'income',
    'i' : 'children',
    'h' : 'living',
    'c' : 'employment',
    'j' : 'income',
    'f' : 'crime', 
    'e' : 'health',
    'd' : 'education',
    'a' : 'IMD',
    'g' : 'housing'
}

def convert_category(c):
    return  conv_cat[c.strip()[0]]

def get_type(fname):
    return fname[8: -4].strip().lower()



def read_2015():
    df = pd.read_csv('./IMD_all_2015.csv')
    # keep only the scores
    df.drop(df.index[df['Measurement'] != 'Score'], inplace=True)
    # drop the unused columns
    dropped_cols = [c.strip() for c in df.columns if c.strip() not in cols]
    df.drop(dropped_cols, axis=1, inplace=True)
    #rename columns
    df.rename(index=str, columns=cols, inplace=True)
    print(df)
    # rename categories
    df['category'] = df['category'].apply(convert_category)

    return df

def read_2010(fname):
    df = pd.read_csv(fname)
    print(df.columns)
    # drop the unused columns
    dropped_cols = [c for c in df.columns if c not in cols]
    df.drop(dropped_cols, axis=1, inplace=True)

    df.rename(index=str, columns=cols, inplace=True)

    df['category'] = get_type(fname)

    return df

def main():

    df = read_2015()
    
    p = Path('.')
    files = [f for f in p.iterdir() if '2010' in f.name]

    for f in files:
        t = read_2010(f.name)
        df = df.append(t, ignore_index=True)

 
    print(df)
    print(df['year'].unique())
    print(df.isnull().values.any())
    df.drop(df.index[df['category'] == ''], inplace=True)

    df.to_csv('combined.csv', index=False)
    





if __name__ == '__main__':
    main()


