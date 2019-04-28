import pandas as pd
from scipy.optimize import curve_fit
import numpy as np
import matplotlib.pyplot as plt
# this is actually all drugs the name on the csv file is wrong
pres = pd.read_csv('../../data/slices/heart/summed_heart.csv')
pres.drop(pres.index[pres.year.isnull()], inplace=True)

TOPK = 200
YEAR = 2015

def all_drugs():
    totals = pres.groupby(['year', 'bnf_code'])\
                    .sum()\
                    .reset_index()\
                    .set_index('year')

    fig, axes = plt.subplots(2,1, figsize=(4, 6))
    all_drugs_ax, heart_ax = axes

    slc = totals.loc[YEAR]
    tot = slc['items'].sum()
    slc = slc.sort_values('items', ascending=False).head(TOPK) 
    slc['items'] /= (tot / 100)

    all_drugs_ax.hist(slc.bnf_code.values.reshape(-1, 1),bins=TOPK, weights=slc['items'].values.reshape(-1, 1))
    all_drugs_ax.set_title(f'All drugs, {YEAR}')
    all_drugs_ax.set_ylabel('% of items')
    all_drugs_ax.set_xticklabels([])
    all_drugs_ax.set_xticks([])


    heart_slc = totals.loc[YEAR]
    heart_slc = heart_slc.loc[heart_slc.bnf_code.apply(lambda x : str(x).startswith('02'))]

    tot = heart_slc['items'].sum()
    heart_slc = heart_slc.sort_values('items', ascending=False).head(TOPK)
    heart_slc['items'] /= (tot / 100)

    heart_ax.hist(heart_slc.bnf_code.values.reshape(-1, 1),bins=TOPK, weights=heart_slc['items'].values.reshape(-1, 1))
    heart_ax.set_title(f'Heart drugs, {YEAR}')
    heart_ax.set_ylabel('% of items')
    heart_ax.set_xticklabels([])
    heart_ax.set_xticks([])

    fig.tight_layout()

    fig.savefig('../pres/figures/drug_dist.png')
    fig.savefig('../figures/drug_dist.png')


def power_law(x, a, b):
    return (a * (x ** b))

def fitted():
    totals = pres.groupby(['year', 'bnf_code'])\
                    .sum()\
                    .reset_index()\
                    .set_index('year')

    #y_ticks = np.arange(0,14,1) * .005
    fig, axes = plt.subplots(2,1, figsize=(4, 6))
    all_drugs_ax, heart_ax = axes

    slc = totals.loc[YEAR]
    tot = slc['items'].sum()
    slc = slc.sort_values('items', ascending=False).head(TOPK) 
    slc['items'] /= (tot / 100)

    slc = slc.reset_index(drop=True)
    xvals = slc.index.values + 1

    popt, pcov  = curve_fit(power_law, xvals, slc['items'].values)
    slc['items'].plot(ax = all_drugs_ax, label='observed')
    fitted = pd.Series(power_law(xvals, *popt))
    fitted.plot(ax = all_drugs_ax, label='fitted')

    all_drugs_ax.set_title(f'All drugs, {YEAR}')
    all_drugs_ax.set_ylabel('% of items')
    all_drugs_ax.set_xticklabels([])
    all_drugs_ax.set_xticks([])
    all_drugs_ax.legend()
    #all_drugs_ax.set_yticks(y_ticks)

    res = slc['items'] - fitted

    heart_slc = totals.loc[YEAR]
    heart_slc = heart_slc.loc[heart_slc.bnf_code.apply(lambda x : str(x).startswith('02'))]

    tot = heart_slc['items'].sum()
    heart_slc = heart_slc.sort_values('items', ascending=False).head(TOPK)
    heart_slc['items'] /= (tot / 100)

    heart_slc = heart_slc.reset_index(drop=True)
    xvals = heart_slc.index.values + 1

    popt, pcov  = curve_fit(power_law, xvals, heart_slc['items'].values)
    heart_slc['items'].plot(ax = heart_ax, label='observed')
    fitted = pd.Series(power_law(xvals, *popt))
    fitted.plot(ax = heart_ax, label='fitted')

    heart_ax.set_title(f'Heart drugs, {YEAR}')
    heart_ax.set_ylabel('% of items')
    heart_ax.set_xticklabels([])
    heart_ax.set_xticks([])
    heart_ax.legend()
    #heart_ax.set_yticks(y_ticks)
    
    fig, ax = plt.subplots()
    res.hist(ax=ax,bins=50)
        
    fig.tight_layout()

    fig.savefig('../pres/figures/drug_dist_fitted.png')
    fig.savefig('../figures/drug_dist_fitted.png')
    

all_drugs()
fitted()





plt.show()
