import pandas as pd
import matplotlib.pyplot as plt

bnf_codes = [
    '0206010K0AAAEAE',
    '0206020F0AAABAB',
    '0206020F0AAACAC',
    '0206020L0AAAAAA',
    '0212000AAAAAAAA'
]

TOPK = 5
# FIXME this was -.20
R_THRES = -.40
PVAL_THRES = .001

def get_outer_code_slices():
    df = pd.read_csv('../heart_map/res.csv')
    neg_corrs = df.loc[(df.rs < R_THRES) & (df.pval < PVAL_THRES)].set_index('bnf_code')

    slcs = {}
    for c in bnf_codes:
        slcs[c] = neg_corrs.loc[c].outer_code.unique().tolist()
        
    return slcs

outer_code_slcs = get_outer_code_slices()

# all the relevant outer_codes
outer_codes = set()
for ocs in outer_code_slcs.values():
    outer_codes |= set(ocs)

pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv')\
                .set_index(['outer_code', 'year'])['total']
pop.name = 'total'


index_cols = ['bnf_code', 'year', 'month']

# national trends
summed = pd.read_csv('../../data/slices/heart/summed_heart.csv')
summed = summed.loc[summed.bnf_code.apply(lambda x :  x in bnf_codes)]

summed['x'] = summed.year + (1/12.0) * (summed.month - 1)
summed = summed.set_index('year').join(pop.reset_index().groupby('year').sum())
summed['items'] /= summed.total


summed = summed.reset_index().set_index('bnf_code')
print(summed)

# raw prescribing data
pres = pd.read_csv('../../data/slices/heart/heart_outer_code.csv')
# get all the rows that we want to graph
pres = pres.loc[pres.bnf_code.apply(lambda x :  x in bnf_codes) & pres.outer_code.apply(lambda x : x in outer_codes)]


pres['x'] = pres.year + (1/12.0) * (pres.month - 1)

pres = pres.set_index(['outer_code', 'year']).join(pop, how='inner')

pres['items'] /= pres.total

pres = pres.reset_index().set_index(['bnf_code', 'outer_code'])

for bc in bnf_codes:  
    summed_slc = summed.loc[bc].sort_values('x')
    plt.plot(summed_slc.x, summed_slc['items'], '^-', linewidth=3, label='National')

    for oc in outer_code_slcs[bc]:
        slc = pres.loc[(bc, oc), : ].sort_values('x')
        plt.plot(slc.x.values, slc['items'].values, linewidth=.1, label=None)

    plt.legend()
    plt.show()




