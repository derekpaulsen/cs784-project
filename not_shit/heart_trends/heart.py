import pandas as pd
from scipy.stats import spearmanr

pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv')
df = pd.read_csv('../../data/slices/heart/heart_outer_code.csv')
# get all the significant codes
code_sums = df.groupby('bnf_code').sum()
#large_codes = set(code_sums.index[code_sums['items'] > 5000000].unique())
large_codes = set(code_sums.sort_values('items', ascending=False).head(10).index.values)

# FIXME account for the population rates


df = df.loc[df.bnf_code.apply(lambda x : x in large_codes)]

overall = df.groupby(['bnf_code', 'year', 'month']).sum().reset_index().set_index('year')
overall = overall.join(pop.groupby('year').sum())

print(overall)

overall['items'] /= overall.total
overall.dropna(inplace=True)
overall = overall.reset_index().set_index(['bnf_code', 'year', 'month'])


print(overall)

outer_codes = df.outer_code.unique()
print(df.bnf_code.unique())
df = df.set_index(['outer_code', 'year']).join(pop.set_index(['outer_code', 'year']))
df['items'] /= df.total

df = df.reset_index().set_index(['bnf_code','outer_code', 'year', 'month'])
df.dropna(inplace=True)


cnt = 0
print('number of combinations', len(large_codes) * len(outer_codes))
res = []
for bc in large_codes:
    print(bc)
    for oc in outer_codes:
        try:
            j = overall.loc[bc].join(df.loc[(bc, oc, slice(None), slice(None)), :], how='inner', rsuffix='_r', lsuffix='_l')
            j.fillna(0.0, inplace=True)

            rs, pval = spearmanr(j['items_l'], j['items_r'])
            res.append((bc, oc,rs, pval, len(j)))

        except KeyError as e:
            print(e)
            
            


print("total count = ", cnt)

with open('res.csv','w') as ofs:
    ofs.write('bnf_code,outer_code,rs,pval,npts\n')
    for t in res:
        ofs.write('{0},{1},{2},{3},{4}\n'.format(*t))
