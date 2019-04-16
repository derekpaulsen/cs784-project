import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import folium
from folium.plugins import HeatMap


TOPK = 10
#df = pd.read_csv('../../data/slices/heart/heart_med_corrs.csv')
df = pd.read_csv('res.csv')
lat_long =  pd.read_csv('../../data/lat_long/outer_codes_lat_long.csv').set_index('outer_code')
neg_corrs = df.loc[(df.rs < -.10) & (df.pval < .001)]
print(neg_corrs.bnf_code.value_counts().sort_values().tail(TOPK))
srted =neg_corrs.bnf_code.value_counts().sort_values().tail(TOPK) 
codes = srted.index.tolist()

for code in codes:
    slc = neg_corrs.loc[neg_corrs.bnf_code == code].set_index('outer_code')
    j = slc.join(lat_long)

    hmap = folium.Map(location=[52.5, -2], zoom_start=7.5)
    #TODO change radius, blur, etc. see the post on the website
    hm_pts = HeatMap(list(zip(j.latitude.values, j.longitude.values, np.abs(j.rs.values))),
                        radius=10,
                        blur=10
                    )
    hmap.add_child(hm_pts)
    hmap.save(f'{code}_test.html')

 #   plt.scatter(j.longitude, j.latitude, np.abs(j.rs.values) * 5)
  #  plt.show()
    
    
all_outer_codes = df.outer_code.unique()
lat_long = lat_long.loc[all_outer_codes, : ]
lat_long.drop(lat_long.index[lat_long.longitude.isnull()], inplace=True)

hmap = folium.Map(location=[52.5, -2], zoom_start=7.5)
hm_pts = HeatMap(list(zip(lat_long.latitude.values, lat_long.longitude.values, np.ones(len(lat_long)))),
                    radius=10,
                    blur=10
                )
hmap.add_child(hm_pts)
hmap.save(f'all_pts.html')

