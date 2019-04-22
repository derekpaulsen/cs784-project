import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn import linear_model
from itertools import product


mortality_cols = [
    'A00-R99,U00-Y89 All causes, all ages',
    'LC27 Hypertensive diseases'
]

    
TOPK = 5
R_THRES = -.20
PVAL_THRES = .001

index_cols = ['outer_code', 'year']

indep_vars = [
    'LC27 Hypertensive diseases',
    'crime',
    'employment',
    'health',
    'housing',
    'income',
    'density',
    'latitude',
    'pop_above_65_percent',
    'pop_above_45_percent'
]   

    



#df = pd.read_csv('../../data/slices/heart/heart_med_corrs.csv')
lat_long =  pd.read_csv('../../data/lat_long/outer_codes_lat_long.csv').set_index('outer_code')

pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv').set_index(index_cols)

density = pd.read_csv('../../data/outer_code_norm/pop_density_by_outer_code.csv').set_index(index_cols)

mor = pd.read_csv('../../data/outer_code_norm/mortality_by_outer_code.csv').set_index(index_cols)[mortality_cols]


imd = pd.read_csv('../../data/outer_code_norm/IMD_by_outer_code.csv')

imd_cats = imd.category.unique().tolist()

imd = imd.set_index(index_cols + ['category']).unstack(['category'])
# remove the value level from the index
imd.columns = pd.Index(imd.columns.levels[1])

imd.drop(imd.index[imd.apply(lambda x : any(x.isnull()), axis=1)], inplace=True)




# get all the data for the codes that we care able
joined = lat_long.join(imd, how='inner')\
                .join(pop, how='inner')\
                .join(density, how='inner')\
                .join(mor, how='inner')


joined['pop_above_65_percent'] = joined['65+'] / joined['total']
joined['pop_above_45_percent'] = (joined['65+'] + joined['45-64']) / joined['total']
joined[ 'LC27 Hypertensive diseases'] /= joined.total

joined = joined.reset_index()
res = []
pairs = [t for t in product(indep_vars, indep_vars) if t[0] < t[1]]
print(joined)
for p in pairs:
    sp, sp_pval = spearmanr(joined[p[0]], joined[p[1]])
    pear, pear_pval = pearsonr(joined[p[0]], joined[p[1]])

    res.append((p[0], p[1], sp, sp_pval, pear, pear_pval))



with open('variable_corrs.csv', 'w') as ofs:
    ofs.write('var1,var2,spearman,spearman_pval,pearson,pearson_pval\n')

    for r in res:
        ofs.write('{0},{1},{2},{3},{4},{5}\n'.format(*r))


