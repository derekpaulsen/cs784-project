import matplotlib.pyplot as plt
import pandas as pd



mor = pd.read_csv('../../data/outer_code_norm/mortality_by_outer_code.csv')
pop = pd.read_csv('../../data/outer_code_norm/population_by_outer_code.csv')



slices = {
    'Heart Related' : [
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
    ], 

    'Accidents' : ['LC01 Accidents'],
    'Cancer' : ['LC02 Cancer (malignant neoplasms)'],
    'Influenza and pneumonia' : ['LC28 Influenza and pneumonia'],
    'Dementia' : ['LC18 Dementia and Alzheimer disease'],
   'Chronic respiratory diseases' : ['LC14 Chronic lower respiratory diseases'],
}


    


def pie_chart():
    drop = ['outer_code']

    deaths = mor.drop(columns=drop).groupby('year').sum().sum()
    all_deaths = mor['A00-R99,U00-Y89 All causes, all ages'].sum()
    slc_total = 0
    pairs = []
    for name, slc in slices.items():
        slc_sum = deaths[slc].sum()
        pairs.append((name, slc_sum))
        slc_total += slc_sum

    pairs.append(('Other', all_deaths - slc_total))
    pairs.sort(key=lambda x : x[1], reverse=True)
    labels = [l for l,s in pairs]
    sizes = [s for l,s in pairs]
    explode = [0] * len(labels)
    explode[labels.index('Heart Related')] = .1



    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90)
    ax1.axis('equal')  
    fig1.savefig('../pres/figures/pie_chart.png')

def time_series():
    
    heart_deaths = mor[slices['Heart Related'] + ['year']].groupby('year').sum().apply(sum, axis=1)
    heart_deaths.name = 'deaths'
    j = pop.groupby('year').sum().join(heart_deaths, how='inner')
    
    DENOM = 10000
    j['deaths'] /= j.total
    j['deaths'] *= DENOM
    fig, ax = plt.subplots()

    j.deaths.plot(ax=ax)
    ax.set_ylabel(f'Deaths per {DENOM}')
    ax.set_xlabel('Year')
    ax.set_title('Heart Related Deaths')
    ax.set_xticks(sorted(list(j.reset_index().year.unique())))

    fig.savefig('../pres/figures/heart_deaths_per_10000.png')




pie_chart()
time_series()
plt.show()
    
