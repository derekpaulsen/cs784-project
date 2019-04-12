import pandas as pd
import matplotlib.pyplot as plt


index_cols = ['year', 'msoa']
pres = pd.read_csv('../../data/msoa_norm/prescribe_msoa.csv').groupby(['year', 'month', 'bnf_sec']).sum().reset_index()

# x_coordinate
start_x = pres.year.min()
pres['x'] = pres.year - start_x + (1/12.0) * pres.month




sections = pres.bnf_sec.unique()

for sec in sections:
    print(sec)
    m = plt.bnf_sec == sec
    plt.plot(plt.loc[m].x, plt.loc[m].items)
    plt.show()






