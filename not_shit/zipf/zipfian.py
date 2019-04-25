import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

def power_law(x, a, b):
    return (a * (x ** b))


YEAR = 2012
NPTS = 400

df = pd.read_csv('../../data/slices/heart/summed_heart.csv').set_index('year')


slc = df.loc[YEAR]
slc['items'] /= slc['items'].sum()

srted = slc['items'].sort_values(ascending=False).reset_index(drop=True).head(NPTS)


print(srted.sum())
xvals = srted.index.values + 1

popt, pcov  = curve_fit(inv_power, xvals, srted.values)


srted.plot()
fitted = pd.Series(inv_power(xvals, *popt))
fitted.plot()

plt.show()


heart_slc = df.loc[df.bnf_code.apply(lambda x : str(x)[:2] == '02')].loc[YEAR]



heart_slc['items'] /= heart_slc['items'].sum()

srted = heart_slc['items'].sort_values(ascending=False).reset_index(drop=True).head(NPTS)


xvals = srted.index.values + 1

popt, pcov  = curve_fit(inv_power, xvals, srted.values)


srted.plot()
fitted = pd.Series(inv_power(xvals, *popt))
fitted.plot()

plt.show()












