import pandas as pd
from scipy.stats import pearsonr
from itertools import product
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, sum
import numpy as np

conf = SparkConf().set("spark.driver.maxResultSize", "100G")\
                    .set('spark.driver.memory', "150G")

sc = SparkContext(master = "local[*]", appName="correlations", conf=conf)
pop = pd.read_csv('/proj/cs784-s19-PG0/outer_code_norm/population_by_outer_code.csv')

df = pd.read_csv('/data/heart_outer_code.csv')
# get all the significant codes
code_sums = df.groupby('bnf_code').sum()
large_codes = set(code_sums.index[code_sums['items'] > 5000000].unique())
df = df.loc[df.bnf_code.apply(lambda x : x in large_codes)]
# only get the large codes
overall = df.groupby(['bnf_code', 'year', 'month']).sum()
overall = overall.loc[(large_codes, slice(None), slice(None)), :]


overall['items'] /= pop.groupby('year').sum().total
print(overall)
overall.drop(overall.index[overall['items'].isnull()], inplace=True)
print(overall)

outer_codes = df.outer_code.unique()
print(df.bnf_code.unique())
df = df.set_index(['outer_code', 'year']).join(pop.set_index(['outer_code', 'year']))


# get the avearge
df['items'] /= df.total

df = df.reset_index().set_index(['bnf_code', 'outer_code', 'year', 'month'])
# drop the nulls
df.drop(df.index[df['items'].isnull()], inplace=True)


print('number of combinations', len(large_codes) * len(outer_codes))

def process(t):
    bc, oc = t
    try:
        j = overall.loc[(bc, slice(None), slice(None)), : ].join(df.loc[(bc, oc, slice(None), slice(None)), :], rsuffix='_r', lsuffix='_l')
        # fill missing values with 0
        j['items_r'].loc[j['items_r'].isnull()] = 0

        rs, pval = pearsonr(j['items_l'], j['items_r'])
        return (bc, oc, rs, pval, len(j))

    except KeyError as e:
        print(e)
        return (bc, oc, np.nan, 1.0, 0)

all_combs = sc.parallelize(product(large_codes, outer_codes))
res = all_combs.map(lambda x : process(x)).collect()
            

with open('res.csv','w') as ofs:
    ofs.write('bnf_code,outer_code,rs,pval,npts\n')
    for t in res:
        ofs.write('{0},{1},{2},{3},{4}\n'.format(*t))
