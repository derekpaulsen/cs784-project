import pandas as pd
import scipy.stats as stats


spearman = stats.spearmanr;
pearson = stats.pearsonr

# bnf_sec 4 is anti depressants

index_cols = ['year', 'msoa']

prescribe_columns = ['quantity','items','net_ingredient_cost','act_cost']

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

j = pres.join(lat_long, how='inner')
print(j)
for sec in sections:
    m = j.bnf_sub_sec == sec
    print(sec)
    print(f'spearman = {spearman(j.latitude.loc[m], j["items"].loc[m])}')
    print(f'pearson = {pearson(j.latitude.loc[m], j["items"].loc[m])}')
    print()






