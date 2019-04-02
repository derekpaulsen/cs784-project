from sklearn import linear_model
import pandas as pd


pop_avgs = pd.read_csv('../../data/pop_avgs/pop_avgs.csv')

def get_wide_imd():
    imd_avgs = pd.read_csv('../../data/outer_code_norm/IMD_outer_code.csv').groupby(['outer_code', 'category']).mean().reset_index()[['outer_code', 'category', 'value']]

    categories = imd_avgs.category.unique()
    first = True
    for cat in categories:
        if first:
            wide = imd_avgs.loc[imd_avgs.category == cat][['outer_code', 'value']]
            wide.rename(columns={'value' : cat + '_val'}, inplace=True)
            first=False
        else:
            df = imd_avgs.loc[imd_avgs.category == cat][['outer_code', 'value']]
            df.rename(columns={'value' : cat + '_val'}, inplace=True)
            wide = wide.merge(df, on='outer_code')

    return wide

def normalize_cols(wide, cols):
    for col in cols:
        mean = wide[col].mean()
        std = wide[col].std()

        wide[col] = (wide[col] - mean) / std

    return wide

wide_imd = get_wide_imd()

val_cols = list(set(wide_imd.columns) - {'outer_code'})
#wide_imd = normalize_cols(wide_imd, val_cols)

heart_avg = pop_avgs.loc[pop_avgs.bnf_sec == 2].groupby('outer_code').mean().reset_index()

combined = wide_imd.merge(heart_avg, on='outer_code')

lm = linear_model.LinearRegression(normalize=True)


lm.fit(combined[val_cols], combined.quantity)


formula = '\n + '.join(f'{c} * {x}' for c,x in zip(lm.coef_, val_cols))
print(formula)

lm = linear_model.LinearRegression(normalize=True)


lm.fit(combined['income_val'].values.reshape(-1,1), combined['quantity'])
print(lm.coef_)
