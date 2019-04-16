import pandas as pd
from scipy.stats import pearsonr

df = pd.read_csv('../../data/heart_outer_code.csv')
# get all the significant codes
code_sums = df.groupby('bnf_code').sum()
large_codes = set(code_sums.index[code_sums['items'] > 5000000].unique())
df = df.loc[df.bnf_code.apply(lambda x : x in large_codes)]

overall = df.groupby(['bnf_code', 'year', 'month']).sum()

outer_codes = df.outer_code.unique()
print(df.bnf_code.unique())
df = df.set_index(['bnf_code','outer_code', 'year', 'month'])


cnt = 0
print('number of combinations', len(large_codes) * len(outer_codes))
res = []
for bc in large_codes:
    for oc in outer_codes:
        j = overall.loc[bc].join(df.loc[(bc, oc, slice(None), slice(None)), :], how='inner', rsuffix='_r', lsuffix='_l')
        rs, pval = pearsonr(j['items_l'], j['items_r'])
        res.append((bc, oc,rs, pval, len(j)))

        if rs < 0 and pval < .05:
            cnt += 1
            print(f'{bc},{oc},{rs},{pval}')
            


print("total count = ", cnt)

with open('res.csv','w') as ofs:
    ofs.write('bnf_code,outer_code,rs,pval,npts\n')
    for t in res:
        ofs.write(f'{0},{1},{2},{3},{4}\n'.format(*t))
