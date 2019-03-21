import pandas as pd
import scipy.stats as stats

spearman = stats.spearmanr;
pearson = stats.pearsonr

pop_avgs = pd.read_csv('../../data/pop_avgs/pop_avgs.csv')
imd = pd.read_csv('../../data/outer_code_norm/IMD_outer_code.csv')


for i in range(1,7):
    print(len(pop_avgs.loc[pop_avgs.bnf_sec == i].merge(imd, on='outer_code')))


