import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# this is actually all drugs the name on the csv file is wrong
pres = pd.read_csv('../../data/slices/heart/summed_heart.csv')
pres.drop(pres.index[pres.year.isnull()], inplace=True)
TOPK = 100

def all_drugs():
    years = sorted(list(pres.year.unique()))
    totals = pres.groupby(['year', 'bnf_code'])\
                    .sum()\
                    .reset_index()\
                    .set_index('year')
    y_ticks = np.arange(0,12,1) * .005
    year = 2017
    fig, axes = plt.subplots(2,1, figsize=(4, 6))
    all_drugs_ax, heart_ax = axes

    slc = totals.loc[year]
    tot = slc['items'].sum()
    slc = slc.sort_values('items').tail(TOPK)
    slc['items'] /= tot

    all_drugs_ax.hist(slc.bnf_code.values.reshape(-1, 1),bins=TOPK, weights=slc['items'].values.reshape(-1, 1))
    all_drugs_ax.set_title(f'All drugs, {year}')
    all_drugs_ax.set_xticklabels([])
    all_drugs_ax.set_xticks([])
    all_drugs_ax.set_yticks(y_ticks)


    heart_slc = totals.loc[year]
    heart_slc = heart_slc.loc[heart_slc.bnf_code.apply(lambda x : str(x).startswith('02'))]

    tot = heart_slc['items'].sum()
    heart_slc = heart_slc.sort_values('items').tail(TOPK)
    heart_slc['items'] /= tot

    heart_ax.hist(heart_slc.bnf_code.values.reshape(-1, 1),bins=TOPK, weights=heart_slc['items'].values.reshape(-1, 1))
    heart_ax.set_title(f'Heart drugs, {year}')
    heart_ax.set_xticklabels([])
    heart_ax.set_xticks([])
    heart_ax.set_yticks(y_ticks)

    

    #fig, axes = plt.subplots(1, 3, figsize=(3, 3))

    #for ax, year in zip(axes, years):
    #    slc = totals.loc[year]
    #    tot = slc['items'].sum()
    #    slc = slc.sort_values('items').tail(TOPK)
    #    slc['items'] /= tot
    #    ax.hist(slc.bnf_code.values.reshape(-1, 1),bins=TOPK, weights=slc['items'].values.reshape(-1, 1))
    #    ax.set_title(str(int(year)))
    #    ax.set_xticklabels([])
    #    ax.set_xticks([])
    #    
    fig.tight_layout()

    fig.savefig('../pres/figures/drug_dist.png')


all_drugs()

plt.show()
