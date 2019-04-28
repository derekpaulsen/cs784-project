import pandas as pd
from scipy.stats import spearmanr

aspirin_codes = pd.read_csv('../../data/BNF_codes/anti_platlet_asp.csv')['BNF Presentation Code']
aspirin_codes = set(aspirin_codes)

df  = pd.read_csv('../../data/slices/heart/heart_outer_code.csv')
df = df.loc[df.bnf_code.apply(lambda x : x in aspirin_codes)]
overall = df.groupby(['year', 'month']).sum()

outer_codes = df.outer_code.unique()

df = df.groupby(['outer_code', 'year', 'month']).sum()


res = []

for oc in outer_codes:
    try:
        j = overall.join(df.loc[(oc, slice(None), slice(None)), :], how='inner', rsuffix='_r', lsuffix='_l')
        j.fillna(0.0, inplace=True)

        rs, pval = spearmanr(j['items_l'], j['items_r'])
        res.append((oc,rs, pval, len(j)))

    except KeyError as e:
        print(e)


with open('res.csv','w') as ofs:
    ofs.write('outer_code,rs,pval,npts\n')
    for t in res:
        ofs.write('{0},{1},{2},{3}\n'.format(*t))
