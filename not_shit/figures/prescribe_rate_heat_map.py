import pandas as pd
import numpy as np
import folium
from folium.plugins import HeatMap

RADIUS = 5
BLUR = 3

index_cols = ['outer_code', 'year']

lat_long = pd.read_csv('../../data/lat_long/outer_codes_lat_long.csv').set_index('outer_code')


pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv').set_index(index_cols)

pres = pd.read_csv('../../data/slices/heart/heart_outer_code.csv').groupby(index_cols + ['bnf_code']).sum().reset_index().set_index(index_cols)

joined = pres.join(pop, how='inner')\
            .join(lat_long, how='inner')

joined['items'] /= joined['total']

joined = joined.reset_index()

years  = joined['year'].unique()

joined = joined.set_index(['year', 'bnf_code'])
code = '0212000AAAAAAAA'
for y in years:
    hmap = folium.Map(location=[52.5, -2], zoom_start=7.5)
    #TODO change radius, blur, etc. see the post on the website
    hm_pts = HeatMap(joined.loc[(y, code), ['latitude', 'longitude', 'items']].values,
                        radius=RADIUS,
                        blur=BLUR
                    )
    hmap.add_child(hm_pts)
    hmap.save(f'{code}_{y}_test.html')






