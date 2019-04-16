import pandas as pd
from scipy.stats import pearsonr, spearmanr
import matplotlib.pyplot as plt


mortality_cols = [
    'A00-R99,U00-Y89 All causes, all ages',
    'LC27 Hypertensive diseases'
]

    
TOPK = 10
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
corrs = pd.read_csv('res.csv')
lat_long =  pd.read_csv('../../data/lat_long/outer_codes_lat_long.csv').set_index('outer_code')

pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv').set_index(index_cols)

density = pd.read_csv('../../data/outer_code_norm/pop_density_by_outer_code.csv').set_index(index_cols)

mor = pd.read_csv('../../data/outer_code_norm/mortality_by_outer_code.csv').set_index(index_cols)[mortality_cols]

pres = pd.read_csv('../../data/slices/heart/heart_outer_code.csv').groupby(index_cols + ['bnf_code']).sum().reset_index().set_index(index_cols)

imd = pd.read_csv('../../data/outer_code_norm/IMD_by_outer_code.csv')

imd_cats = imd.category.unique().tolist()

imd = imd.set_index(index_cols + ['category']).unstack(['category'])
# remove the value level from the index
imd.columns = pd.Index(imd.columns.levels[1])

imd.drop(imd.index[imd.apply(lambda x : any(x.isnull()), axis=1)], inplace=True)



# all the data pts that don't follow the national trend
neg_corrs = corrs.loc[(corrs.rs < -.10) & (corrs.pval < .001)]

print(neg_corrs.bnf_code.value_counts().sort_values().tail(TOPK))
srted =neg_corrs.bnf_code.value_counts().sort_values().tail(TOPK) 
codes = set(srted.index.tolist())

# get all the data for the codes that we care able
pres = pres.loc[pres.bnf_code.apply(lambda x : x in codes)]
joined = pres.join(lat_long, how='inner')\
                .join(imd, how='inner')\
                .join(pop, how='inner')\
                .join(density, how='inner')\
                .join(mor, how='inner')

joined['items'] /= joined.total

joined['pop_above_65_percent'] = joined['65+'] / joined['total']
joined['pop_above_45_percent'] = (joined['65+'] + joined['45-64']) / joined['total']
joined[ 'LC27 Hypertensive diseases'] /= joined.total

joined = joined.reset_index().set_index('bnf_code')

for code in codes:
    slc = joined.loc[code]
    for v in indep_vars:
        print(code)
        sp, sp_pval = spearmanr(slc['items'], slc[v])
        pear, pear_pval = pearsonr(slc['items'], slc[v])
        print(f'{v} : spearman = ({sp}, {sp_pval}), pearson = ({pear}, {pear_pval})\n')
        
    print('\n\n')
