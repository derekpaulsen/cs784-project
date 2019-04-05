import pandas as pd
import scipy.stats as stats
from sklearn import linear_model


spearman = stats.spearmanr;
pearson = stats.pearsonr

# bnf_sec 14 is vaccines and what not
FLU = 'LC28 Influenza and pneumonia'

index_cols = ['year', 'msoa']

prescribe_columns = ['quantity','items','net_ingredient_cost','act_cost']


mor = pd.read_csv('../../data/msoa_norm/mortality_by_msoa.csv').set_index(index_cols)
pop = pd.read_csv('../../data/msoa_norm/population_by_msoa.csv').set_index(index_cols)
pres = pd.read_csv('../../data/msoa_norm/prescribe_msoa.csv')

imd = pd.read_csv('../../data/msoa_norm/IMD_by_msoa.csv')
imd_cats = imd.category.unique().tolist()

imd = imd.set_index(index_cols + ['category']).unstack(['category'])
# take only the points from 2010 and 2015, since they are actual measurements
imd = imd.loc[[2010, 2015]]
# remove the value level from the index
imd.columns = pd.Index(imd.columns.levels[1])


vaccines = pres.loc[pres.bnf_sec == 14]

vaccines = vaccines.groupby(index_cols).sum()

# get the averages, drop the nulls 
for col in prescribe_columns:
    vaccines[col] /= pop.total
    vaccines.drop(vaccines.index[vaccines[col].isnull()], inplace=True)

# death per 1000 people
flu_deaths = mor[FLU] / pop.total * 1000
flu_deaths.name = FLU


joined = vaccines.join(flu_deaths).join(imd)
nulls = joined.isnull().apply(lambda x :  x.any(), axis=1)
joined.drop(joined.index[nulls], inplace=True)

#normalize the IMD scores
joined[imd_cats] -= joined[imd_cats].mean()
joined[imd_cats] /= joined[imd_cats].std()



def process(joined, col):
    
    joined.drop(joined.index[joined[col].isnull()], inplace=True)

    sp, sp_pval = spearman(joined[FLU], joined[col])
    pear, pear_pval = pearson(joined[FLU], joined[col])
    print(f'{col} : spearman = ({sp}, {sp_pval}), pearson = ({pear}, {pear_pval})')
    print()

    lm = linear_model.LinearRegression()
    lm.fit(joined[col].values.reshape(-1,1), joined[FLU])
    rs = lm.score(joined[col].values.reshape(-1,1), joined[FLU])
    print(f'coef WITHOUT IMD = {str(lm.coef_)}')
    print(f'R^2 = {rs}')
    print()
    lm = linear_model.LinearRegression()
    all_cols = [col] + imd_cats
    lm.fit(joined[all_cols], joined[FLU])
    rs = lm.score(joined[all_cols], joined[FLU])
    print('\n+ '.join([f'{c} * {x}' for c,x in zip(lm.coef_, all_cols)]))
    print(f'R^2 = {rs}')
    print('\n\n')
    

    

    

for col in prescribe_columns:
    process(joined, col)








