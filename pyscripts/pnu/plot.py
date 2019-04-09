import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as stats



# bnf_sec 14 is vaccines and what not
FLU = 'LC28 Influenza and pneumonia'

index_cols = ['year', 'msoa']

prescribe_columns = ['quantity','items','net_ingredient_cost','act_cost']


mor = pd.read_csv('../../data/msoa_norm/mortality_by_msoa.csv').set_index(index_cols)
pop = pd.read_csv('../../data/msoa_norm/population_by_msoa.csv').set_index(index_cols)
pres = pd.read_csv('../../data/msoa_norm/prescribe_msoa.csv')

vaccines = pres.loc[pres.bnf_sec == 14]

vaccines = vaccines.groupby(index_cols).sum()

# get the averages, drop the nulls 
for col in prescribe_columns:
    vaccines[col] /= pop.total
    vaccines.drop(vaccines.index[vaccines[col].isnull()], inplace=True)

flu_deaths = mor[FLU] / pop.total
flu_deaths.name = FLU


vaccines.hist(bins=50)
plt.show()

flu_deaths.hist(bins=50)
plt.show()








