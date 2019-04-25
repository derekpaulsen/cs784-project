import pandas as pd
import matplotlib.pyplot as plt


pres = pd.read_csv('../../data/slices/heart/summed_heart.csv')
pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv').groupby('year').sum()
mor = pd.read_csv('../../data/outer_code_norm/mortality_by_outer_code.csv').groupby('year').sum()


mor = mor.join(pop)



demi = pres.loc[pres.bnf_code.apply(lambda  x : str(x).startswith('0411'))]\
                .groupby(['year', 'month']).sum()\
                .join(pop)\
                .reset_index()

drug_color = 'tab:blue'
death_color= 'tab:red'


demi['items'] /= demi.total
demi['x'] = demi['year'] + (demi['month'] - 1) / 12

demi = demi.loc[(demi.x >= 2013) & (demi.x <= 2016)]

dfig, dax = plt.subplots()
dax.set_title('Dementia')

dax.plot(demi['x'].values, demi['items'].values, label='Prescriptions', color='tab:blue')
dax.set_ylabel('Items per Person', color=drug_color)
dax.tick_params(axis='y', labelcolor=drug_color)



dax2 = dax.twinx()

mor['LC18 Dementia and Alzheimer disease'] /= (mor.total / 10000)
mor['LC18 Dementia and Alzheimer disease'].plot(ax = dax2, label='Deaths', color='tab:red')

dax2.set_ylabel('Deaths Per 10,000', color=death_color)
dax2.tick_params(axis='y', labelcolor=death_color)
dax.set_xlim((2013, 2016))

dax.set_xticks(list(range(2013, 2017)))
dax.set_xlabel('Year')

dfig.tight_layout()

heart_related = [
   'LC24 Heart failure and complications and ill-defined heart disease',
   'LC06 Atherosclerosis',
   'LC37 Pulmonary heart disease and diseases of pulmonary circulation',
   'LC27 Hypertensive diseases',
   'LC15 Chronic rheumatic heart diseases',
   'LC12 Cerebrovascular diseases',
   'LC08 Cardiac arrest',
   'LC09 Cardiac arrhythmias',
   'LC10 Cardiomyopathy',
   'LC30 Ischaemic heart diseases'
]


heart = pres.loc[pres.bnf_code.apply(lambda  x : str(x).startswith('02'))]\
                .groupby(['year', 'month']).sum()\
                .join(pop)\
                .reset_index()


heart['items'] /= heart.total
heart['x'] = heart['year'] + (heart['month'] - 1) / 12

heart = heart.loc[(heart.year >= 2013) & (heart.x <= 2016)]



hfig, hax = plt.subplots()

hax.set_title('Heart Related')

hax.plot(heart['x'].values, heart['items'].values, label='Prescriptions', color=drug_color)
hax.set_ylabel('Items per Person', color=drug_color)
hax.tick_params(axis='y', labelcolor=drug_color)


hax2 = hax.twinx()

heart_deaths = mor[heart_related].apply(lambda x : x.sum(), axis=1)

heart_deaths /= (pop.total /10000)
heart_deaths.plot(ax = hax2, label='Deaths', color=death_color)

hax2.set_ylabel('Deaths Per 10,000', color=death_color)
hax2.tick_params(axis='y', labelcolor=death_color)
hax.set_xlim((2013, 2016))

hax.set_xticks(list(range(2013, 2017)))
hax.set_xlabel('Year')


dfig.tight_layout()
hfig.tight_layout()

dfig.savefig('../figures/dementia_death_vs_drugs.png')
hfig.savefig('../figures/heart_death_vs_drugs.png')

plt.show()


