import pandas as pd
import scipy.stats as stats
from sklearn import linear_model


spearman = stats.spearmanr;
pearson = stats.pearsonr

DEATH = 'deaths'

index_cols = ['year', 'msoa']

prescribe_columns = ['quantity','items','net_ingredient_cost','act_cost']

# mortality for pnu
mor = pd.read_csv('../../data/msoa_norm/pneumonia_deaths_2013_2017_msoa.csv').set_index(index_cols)
#population 
pop = pd.read_csv('../../data/msoa_norm/population_by_msoa.csv').set_index(index_cols)

density = pd.read_csv('../../data/msoa_norm/pop_density_by_msoa.csv').set_index(index_cols)['density']
density.name = 'density'

pres = pd.read_csv('../../data/msoa_norm/penicillin_msoa.csv')
pres = pres.groupby(index_cols).sum()

# IMD 
imd = pd.read_csv('../../data/msoa_norm/IMD_by_msoa.csv')
imd_cats = imd.category.unique().tolist()

imd = imd.set_index(index_cols + ['category']).unstack(['category'])
# remove the value level from the index
imd.columns = pd.Index(imd.columns.levels[1])



# get the averages, drop the nulls 
for col in prescribe_columns:
    pres[col] /= pop.total
    pres.drop(pres.index[pres[col].isnull()], inplace=True)

# death per 1000 people
pnu_deaths = mor[DEATH] / pop.total * 1000
pnu_deaths.name = DEATH


joined = pres.join(pnu_deaths).join(imd).join(density)
nulls = joined.isnull().apply(lambda x :  x.any(), axis=1)
joined.drop(joined.index[nulls], inplace=True)

#normalize the IMD scores
joined[imd_cats] -= joined[imd_cats].mean()
joined[imd_cats] /= joined[imd_cats].std()

#TODO plot the residuals to see if they are normally distributed, in addtion make a QQplot for the
# IMDS to make sure that normalizing them makes sense

def process(joined, col, independant_vars):
    sp, sp_pval = spearman(joined[DEATH], joined[col])
    pear, pear_pval = pearson(joined[DEATH], joined[col])
    print(f'{col} : spearman = ({sp}, {sp_pval}), pearson = ({pear}, {pear_pval})')
    print()

    lm = linear_model.LinearRegression()
    lm.fit(joined[col].values.reshape(-1,1), joined[DEATH])
    rs = lm.score(joined[col].values.reshape(-1,1), joined[DEATH])
    print(f'coef WITHOUT IMD = {str(lm.coef_)}')
    print(f'R^2 = {rs}')
    print()


    all_cols = [col] + independant_vars 

    lm = linear_model.LinearRegression()
    lm.fit(joined[all_cols], joined[DEATH])
    rs = lm.score(joined[all_cols], joined[DEATH])
    print('\n+ '.join([f'{c} * {x}' for c,x in zip(lm.coef_, all_cols)]))
    print(f'R^2 = {rs}')
    print('\n\n')

def process_two_vars(joined, col, independant_vars):
    print(col)
    lm = linear_model.LinearRegression()
    lm.fit(joined[col], joined[DEATH])
    rs = lm.score(joined[col], joined[DEATH])
    print(f'coef WITHOUT IMD = {str(lm.coef_)}')
    print(f'R^2 = {rs}')
    print()


    all_cols = col + independant_vars 

    lm = linear_model.LinearRegression()
    lm.fit(joined[all_cols], joined[DEATH])
    rs = lm.score(joined[all_cols], joined[DEATH])
    print('\n+ '.join([f'{c} * {x}' for c,x in zip(lm.coef_, all_cols)]))
    print(f'R^2 = {rs}')
    print('\n\n')

    

all_indep_vars = set(imd_cats + ['density'] + prescribe_columns)
for v in all_indep_vars:
    confouding_vars = all_indep_vars - {v}
    process(joined, v, list(confouding_vars))


for col in prescribe_columns:
    v = {col, 'density'}
    confouding_vars = all_indep_vars - v
    process_two_vars(joined, list(v), list(confouding_vars))

    







