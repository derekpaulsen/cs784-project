import pandas as pd
from scipy.stats import pearsonr
from itertools import product
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, sum

conf = SparkConf().set("spark.driver.maxResultSize", "100G")\
                    .set('spark.driver.memory', "150G")

sc = SparkContext(master = "local[*]", appName="correlations", conf=conf)
pop = pd.read_csv('/proj/cs784-s19-PG0/outer_code_norm/population_by_outer_code.csv')

df = pd.read_csv('/data/heart_outer_code.csv')
# get all the significant codes
code_sums = df.groupby('bnf_code').sum()
large_codes = set(code_sums.index[code_sums['items'] > 5000000].unique())
df = df.loc[df.bnf_code.apply(lambda x : x in large_codes)]

overall = df.groupby(['bnf_code', 'year', 'month']).sum()
overall /= pop.groupby('year').sum()

outer_codes = df.outer_code.unique()
print(df.bnf_code.unique())
df = df.set_index(['bnf_code','outer_code', 'year', 'month'])
# get the avearge
df['items'] /= pop.set_index(['outer_code', 'year']).total
# drop the nulls
df.drop(df.index[df['items'].isnull()], inplace=True)


print('number of combinations', len(large_codes) * len(outer_codes))

def process(t):
    bc, oc = t
    j = overall.loc[bc].join(df.loc[(bc, oc, slice(None), slice(None)), :], rsuffix='_r', lsuffix='_l')
    # fill missing values with 0
    j['items_r'].loc[j['items_r'].isnull()] = 0

    rs, pval = pearsonr(j['items_l'], j['items_r'])
    return (bc, oc, rs, pval, len(j))

all_combs = sc.parallelize(product(large_codes, outer_codes))
res = all_combs.map(lambda x : process(x)).collect()
            

with open('res.csv','w') as ofs:
    ofs.write('bnf_code,outer_code,rs,pval,npts\n')
    for t in res:
        ofs.write('{0},{1},{2},{3},{4}\n'.format(*t))
