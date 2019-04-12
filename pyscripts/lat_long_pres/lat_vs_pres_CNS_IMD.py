import pandas as pd
import scipy.stats as stats

from sklearn import linear_model

spearman = stats.spearmanr;
pearson = stats.pearsonr

ITEMS = 'items'
NORMALIZE = True

def process(joined, col, independant_vars):
    sp, sp_pval = spearman(joined[ITEMS], joined[col])
    pear, pear_pval = pearson(joined[ITEMS], joined[col])
    print(f'{col} : spearman = ({sp}, {sp_pval}), pearson = ({pear}, {pear_pval})')
    print()

    lm = linear_model.LinearRegression(normalize=NORMALIZE)
    lm.fit(joined[col].values.reshape(-1,1), joined[ITEMS])
    rs = lm.score(joined[col].values.reshape(-1,1), joined[ITEMS])
    init = lm.coef_[0]
    print(f'coef WITHOUT IMD = {str(lm.coef_)}')
    print(f'R^2 = {rs}')
    print()


    all_cols = [col] + independant_vars 
    lm = linear_model.LinearRegression(normalize=NORMALIZE)
    lm.fit(joined[all_cols], joined[ITEMS])
    rs = lm.score(joined[all_cols], joined[ITEMS])
    comb = lm.coef_[0]
    print('\n+ '.join([f'{c} * {x}' for c,x in zip(lm.coef_, all_cols)]))
    print(f'R^2 = {rs}')

    if abs(init-comb) < init/10:
        print('\n!!!SIG!!!!')
    print('\n\n')


# bnf_sec 4 is anti depressants

index_cols = ['year', 'msoa']
prescribe_columns = ['quantity','items','net_ingredient_cost','act_cost']


imd = pd.read_csv('../../data/msoa_norm/IMD_by_msoa.csv')
imd_cats = imd.category.unique().tolist()

imd = imd.set_index(index_cols + ['category']).unstack(['category'])
# remove the value level from the index
imd.columns = pd.Index(imd.columns.levels[1])

imd = imd.groupby('msoa').mean()

density = pd.read_csv('../../data/msoa_norm/pop_density_by_msoa.csv').groupby('msoa').mean()['density']
density.name = 'density'


lat_long = pd.read_csv('../../data/msoa_norm/lat_long_msoa.csv').set_index('msoa')
pop = pd.read_csv('../../data/msoa_norm/population_by_msoa.csv').groupby('msoa').mean()
pres = pd.read_csv('../../data/msoa_norm/prescribe_msoa.csv')
pres = pres.loc[pres.bnf_sec == 4]
pres = pres.groupby(['msoa', 'bnf_sub_sec']).sum()





# get the averages, drop the nulls 
for col in prescribe_columns:
    pres[col] /= pop.total
    pres.drop(pres.index[pres[col].isnull()], inplace=True)

pres = pres.reset_index().set_index('msoa')
sections = pres.bnf_sub_sec.unique()

j = pres.join(lat_long, how='inner').join(imd, how='inner').join(density, how='inner')


all_vars = set(imd_cats + ['density','latitude'])
print(j)
for sec in sections:
    print(f"SUB SEC = {sec}")
    m = j.bnf_sub_sec == sec
    joined = j.loc[m]
    for v in all_vars:
        confouding_vars = all_vars - {v}
        process(joined, v, list(confouding_vars))






