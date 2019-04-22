import pandas as pd
import matplotlib.pyplot as plt

pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv')
mor = pd.read_csv('../../data/outer_code_norm/mortality_by_outer_code.csv')
# this is actually all drugs the name on the csv file is wrong
pres = pd.read_csv('../../data/slices/heart/summed_heart.csv')
pres.drop(pres.index[pres.year.isnull()], inplace=True)


heart_meds = pres.loc[pres.bnf_code.apply(lambda  x : str(x).startswith('02'))]\
                    .groupby(['year', 'month']).sum()\
                    .join(pop.groupby('year').sum(), how='inner')

heart_meds['items'] /= heart_meds.total
heart_meds = heart_meds.reset_index()
years = sorted(list(heart_meds.year.unique()))
heart_meds['x'] = heart_meds.year + (heart_meds.month -1 ) * (1/12.0)

plt.plot(heart_meds['x'], heart_meds['items'])
plt.xticks(years)
plt.ylabel('Items per person per month')
plt.xlabel('Year')
plt.title('Heart Medication Prescribing Rate')
plt.savefig('../pres/figures/heart_med_rate_over_time.png')
plt.show()


