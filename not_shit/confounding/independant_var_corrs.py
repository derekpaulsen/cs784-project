import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn import linear_model
from itertools import product


mortality_cols = [
    'A00-R99,U00-Y89 All causes, all ages',
    'LC27 Hypertensive diseases'
]

heart_rel = [
   'LC24 Heart failure and complications and ill-defined heart disease',
   'LC06 Atherosclerosis',
   'LC37 Pulmonary heart disease and diseases of pulmonary circulation',
   'LC27 Hypertensive diseases',
   'LC15 Chronic rheumatic heart diseases',
   'LC12 Cerebrovascular diseases',
   'LC08 Cardiac arrest',
   'LC09 Cardiac arrhythmias',
   'LC10 Cardiomyopathy',
   'LC30 Ischaemic heart diseases'
]
    
TOPK = 5
R_THRES = -.20
PVAL_THRES = .001

index_cols = ['outer_code', 'year']

indep_vars = [
    'heart_related',
    'crime',
    'employment',
    'health',
    'housing',
    'income',
    'density',
    'latitude',
    'pop_above_65',
    'pop_above_45'
]   

    



#df = pd.read_csv('../../data/slices/heart/heart_med_corrs.csv')
lat_long =  pd.read_csv('../../data/lat_long/outer_codes_lat_long.csv').set_index('outer_code')

pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv').set_index(index_cols)

density = pd.read_csv('../../data/outer_code_norm/pop_density_by_outer_code.csv').set_index(index_cols)

mor = pd.read_csv('../../data/outer_code_norm/mortality_by_outer_code.csv').set_index(index_cols)[heart_rel]


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


joined['pop_above_65'] = joined['65+'] / joined['total']
joined['pop_above_45'] = (joined['65+'] + joined['45-64']) / joined['total']
joined['heart_related'] = joined[heart_rel].apply(lambda x : x.sum(), axis=1) / joined.total

joined = joined.reset_index()
res = {}

print(joined)

for x in indep_vars:
    for y in indep_vars:
        sp, sp_pval = spearmanr(joined[x], joined[y])
        pear, pear_pval = pearsonr(joined[x], joined[y])

        res[(x,y)] = (sp, sp_pval, pear, pear_pval)


with open('res.tex', 'w') as ofs:
    # header
    ofs.write('\\begin{tabular}{')
    ofs.write('|c' * (len(indep_vars) + 1))
    ofs.write('|}\n')

    ofs.write('\t\hline\n')
    ofs.write('\t' + ' & '.join([s.replace('_', '\\_') for s in indep_vars]) + '\\\\\n')

    for x in indep_vars:
        ofs.write('\t\hline\n')
        ofs.write('\t'+x.replace('_', '\\_'))
        for y in indep_vars:
            ofs.write(f'& %.3f' % res[(x,y)][2])
        ofs.write('\\\\ \n')
            

    ofs.write('\t\hline\n')
    ofs.write('\\end{tabular}\n')
