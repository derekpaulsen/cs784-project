import pandas as pd
from scipy.stats import normaltest
import matplotlib.pyplot as plt

TOPK = 10
pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv')
pres = pd.read_csv('../../data/slices/heart/heart_outer_code.csv')\
            .groupby(['year', 'outer_code', 'bnf_code']).sum()


most_pres = pres.groupby('bnf_code').sum()\
                .sort_values('items')\
                .tail(TOPK)\
                .index
most_pres = list(reversed(list(most_pres)))
print(most_pres)

pop = pop.groupby(['year', 'outer_code']).sum()
j = pres.loc[(slice(None), slice(None), most_pres), :]\
            .join(pop)


j['items'] /= j.total
        

for code in most_pres:
    for year in range(2011, 2012):
        slc = j.loc[(year, slice(None), code), 'items']
        slc.drop(slc.index[slc.isnull()], inplace=True)
        slc.hist(bins=30)
        plt.title(f'{code}, {year}')

        no_outliers = slc.drop(slc.index[((slc - slc.mean()) / slc.std()).apply(lambda x : abs(x) > 3)])
        print(f'{code}, {year}, {normaltest(no_outliers.values)}')
        plt.show()



