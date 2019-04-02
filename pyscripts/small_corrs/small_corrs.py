import pandas as pd
import scipy.stats as stats


# TODO interpolate the IMD's using a linear model calculate the correlations 
# aligning on the year and see what happens with the correlations, compare the diff
# between those and the average correlations.

# TODO spearman and pearson corr
# do linear regression with a basic linear model and look at the beta hat's
# perform stat test to see if there is a significant interation between 
# imd and drugs prescribed. 

# TODO look at mortality compared to IMD, mortality compared to population avg  prescribing data
#      find stat test for looking at interations between the three, maybe worse imd means that less 
# people are getting medication for the diseases
# 
# look for the obvious interations, i.e. corr between meds and mortality and mortality and imd go from there

spearman = stats.spearmanr;
pearson = stats.pearsonr

pop_avgs = pd.read_csv('../../data/pop_avgs/pop_avgs.csv')
imd = pd.read_csv('../../data/outer_code_norm/IMD_outer_code.csv')

imd_avgs = imd[['category', 'outer_code', 'value']].groupby(['category', 'outer_code']).mean().reset_index()

categories = imd.category.unique()
outer_codes = imd.outer_code.unique()

def corr_func(x):
    sp, sp_pval = spearman(x.value, x.quantity)
    pear, pear_pval = pearson(x.value, x.quantity)
    return pd.Series({
        'spearman' : sp,
        'spearman_pval' : sp_pval,
        'abs_spearman' : abs(sp),
        'pearson' : pear,
        'pearson_pval' : pear_pval,
        'abs_pearson' : abs(pear),
        'npts' : len(x)
    })

def interpolate(r):
    try:
        v10 = r.loc[r.year == 2010][0].value
        v15 = r.loc[r.year == 2015][0].value
        delta = v15 - v10
        #FIXME this is incorrect
        return [v10, v10 + delta/4, v10 + delta/2, v15 - delta/4, v15]
    except:
        return []

bnf_secs = pop_avgs.bnf_sec.unique()
corrs = []
for bs in bnf_secs:
    filtered_pop_avgs = pop_avgs.loc[pop_avgs.bnf_sec == bs].merge(imd_avgs, on='outer_code')
    corrleations = filtered_pop_avgs.groupby(['bnf_sec', 'bnf_sub_sec', 'category']).apply(corr_func)
    corrs.append(corrleations.reset_index())

agg = pd.concat(corrs, ignore_index=True)
agg.drop(agg.index[agg.spearman.isnull()], inplace=True)

agg.to_csv('agg.csv',index=False)

#interpolated = imd.groupby(['outer_code', 'category']).apply(interpolate).reset_index()
#interpolated.drop(interpolated.index[interpolated[0].apply(lambda x : len(x) == 5)], inplace=True)
#
#print(len(interpolated))





#for i in range(1,7):
#    print(len(pop_avgs.loc[pop_avgs.bnf_sec == i].merge(imd, on='outer_code')))


