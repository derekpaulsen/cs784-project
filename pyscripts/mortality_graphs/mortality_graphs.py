import pandas as pd
import matplotlib.pyplot as plt

index_cols = ['year', 'msoa']

mor = pd.read_csv('../../data/msoa_norm/mortality_by_msoa.csv').set_index(index_cols)
pop = pd.read_csv('../../data/msoa_norm/population_by_msoa.csv').set_index(index_cols)


combined = mor.join(pop)
causes = mor.columns[:-1]
avgs = [x + '_avg' for x in causes]
print(causes)

for c in causes:
    combined[c + '_avg'] = combined[c] / combined.total

print(combined)
print(combined[avgs])
print(combined[avgs].groupby('year').mean())



for c in avgs:
    print(c)
    combined[c].groupby('year').mean().plot()    
    plt.show()




