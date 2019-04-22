import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import normaltest


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


def f(x):
    stat , pval = normaltest(x.rs.values)

    return pd.Series({'sval' : stat, 'pval' : pval})


#df = pd.read_csv('../../data/slices/heart/heart_med_corrs.csv')
corrs = pd.read_csv('../heart_map/res.csv').dropna()

corrs['rs'] = corrs.rs ** 2


df  = corrs.groupby('bnf_code').apply(f).sort_values('pval')

print(df)
corrs = corrs.set_index('bnf_code')

for c in df.index.values:
    corrs.loc[c].rs.hist(bins=50)
    plt.title(str(df.loc[c]))
    plt.show()





