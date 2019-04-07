import pandas as pd



cdf = pd.read_csv('combined.csv')

map_df = pd.read_csv('../linking/Postcode_to_Output_Area_to_Lower_Layer_Super_Output_Area_to_Middle_Layer_Super_Output_Area_to_Local_Authority_District_February_2018_Lookup_in_the_UK.csv')
# drop everything except for the lsoa and the post codes
map_df = map_df[['lsoa11cd', 'pcds']]

m = dict(zip(map_df['lsoa11cd'], map_df['pcds'].apply(lambda x: x.split()[0])))

cdf['outer_code'] = cdf['lsoa'].apply(lambda x: m.get(x.strip(), None))

print(cdf.outer_code.apply(lambda x : x == None).sum())
print(cdf.head())

cdf.drop(columns=['lsoa'], inplace=True)

cdf.to_csv('IMD_outercode_2010_2015.csv', index=False)
