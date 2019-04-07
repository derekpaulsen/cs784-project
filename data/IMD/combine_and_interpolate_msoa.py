import pandas as pd


msoa_map = {}

df = pd.read_csv('../linking/Postcode_to_Output_Area_to_Lower_Layer_Super_Output_Area_to_Middle_Layer_Super_Output_Area_to_Local_Authority_District_February_2018_Lookup_in_the_UK.csv')


msoa_map.update(
    dict(zip(df.lsoa11cd, df.msoa11cd))
)

cdf = pd.read_csv('combined.csv')
# drop everything except for the lsoa and the post codes


cdf['msoa'] = cdf['lsoa'].apply(lambda x: msoa_map.get(x.strip(), None))

print(cdf.msoa.apply(lambda x : x == None).sum())
print(cdf.head())

cdf.drop(columns=['lsoa'], inplace=True)
cdf = cdf.groupby(['msoa', 'category', 'year']).mean().reset_index()

cdf  = cdf.set_index(['year', 'msoa', 'category'])

parts = {}
parts[2010] = cdf.loc[2010]
parts[2015] = cdf.loc[2015]
deltas = parts[2015] - parts[2010]
print(deltas)
print(parts[2010])
parts[2011] = parts[2010] + deltas * .2
parts[2012] = parts[2010] + deltas * .4
parts[2013] = parts[2010] + deltas * .6
parts[2014] = parts[2010] + deltas * .8

dfs = []
for k,v in parts.items():
    v['year'] = k
    v = v.reset_index()
    dfs.append(v)

print(dfs[0])
agg = pd.concat(dfs, ignore_index=True)

prop_nulls = agg.groupby('category').apply(lambda x : x.value.isnull().sum() / len(x))
too_many_nulls = set(prop_nulls.index[prop_nulls > .5])
agg.drop(agg.index[agg.category.apply(lambda x : x in too_many_nulls)], inplace=True)

agg = agg.reset_index()
agg.drop(columns=['index'], inplace=True)
agg.to_csv('IMD_by_msoa.csv',index=False)
agg.to_csv('../msoa_norm/IMD_by_msoa.csv', index=False)
