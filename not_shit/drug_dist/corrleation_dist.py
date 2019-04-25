import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import pearsonr, spearmanr
from sklearn import linear_model


mortality_cols = [
    'A00-R99,U00-Y89 All causes, all ages',
    'LC27 Hypertensive diseases'
]

 
most_prescribed = ['0212000Y0AAADAD', '0209000A0AAABAB', '0206020A0AAAAAA', '0202010B0AAABAB', '0212000Y0AAABAB', '0205051R0AAADAD', '0206020A0AAABAB', '0212000B0AAABAB', '0202020L0AABDBD', '0212000B0AAACAC']

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
corrs = pd.read_csv('../heart_map/res.csv')
#lat_long =  pd.read_csv('../../data/lat_long/outer_codes_lat_long.csv').set_index('outer_code')
#
#pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv').set_index(index_cols)
#
#density = pd.read_csv('../../data/outer_code_norm/pop_density_by_outer_code.csv').set_index(index_cols)
#
#mor = pd.read_csv('../../data/outer_code_norm/mortality_by_outer_code.csv').set_index(index_cols)[mortality_cols]
#
#pres = pd.read_csv('../../data/slices/heart/heart_outer_code.csv').groupby(index_cols + ['bnf_code']).sum().reset_index().set_index(index_cols)
#
#imd = pd.read_csv('../../data/outer_code_norm/IMD_by_outer_code.csv')
#
#imd_cats = imd.category.unique().tolist()
#
#imd = imd.set_index(index_cols + ['category']).unstack(['category'])
## remove the value level from the index
#imd.columns = pd.Index(imd.columns.levels[1])
#
#imd.drop(imd.index[imd.apply(lambda x : any(x.isnull()), axis=1)], inplace=True)



# all the data pts that don't follow the national trend
#neg_corrs = corrs.loc[(corrs.rs < R_THRES)]
#
#print(neg_corrs.bnf_code.value_counts().sort_values().tail(TOPK))
#srted = neg_corrs.bnf_code.value_counts().sort_values().tail(TOPK) 
#codes = srted.index.tolist()
#codes += neg_corrs.bnf_code.value_counts().sort_values().head(TOPK).index.tolist()
#
#
corrs = corrs.set_index('bnf_code')
#print(codes)
#


mu_sig = corrs.groupby('bnf_code').apply(lambda x : pd.Series({'mu' : x.rs.mean(), 'std' : x.rs.std()}))

min_mu = mu_sig.sort_values('mu').head(TOPK)
print(min_mu)
codes = min_mu.index.tolist()

max_std = mu_sig.sort_values('std').tail(TOPK)
min_std = mu_sig.sort_values('std').head(TOPK)

codes = max_std.index.tolist() + min_std.index.tolist()
codes = most_prescribed
for c in codes:
    corrs.loc[c].rs.hist(bins=50, histtype='step', density=True)
    plt.title(c)
plt.show()


    
corrs.rs.hist(bins=50)
plt.show()
