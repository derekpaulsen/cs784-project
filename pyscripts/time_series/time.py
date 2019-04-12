import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


df = pd.read_csv('../../data/summed.csv')
df['x'] = df.year + (1/12.0) * df.month
ticks = df.x.unique()
ticks = ticks[~np.isnan(ticks)]
ticks = sorted(list(ticks))[::3]

codes = sorted([str(x).strip() for x in df.bnf_code.unique()])

#heart disease meds
codes = [c for c in codes if c[0:2] == '02']

for code in codes:
    print(code)
    m = df.bnf_code == code
    slc = df.loc[m].sort_values('x')
    plt.plot(slc.x, slc['items'])
    plt.xticks(ticks)
    plt.title(code)
    plt.show()
    
