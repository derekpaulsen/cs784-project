import pandas as pd
import matplotlib.pyplot as plt


df  = pd.read_csv('../../data/summed.csv')
years = list(range(2012, 2018))
TOPK = 5

df = df.groupby(['year', 'bnf_code']).sum().reset_index().set_index('year')
df['bnf_sec'] = df.bnf_code.apply(lambda x : int(str(x)[0:2]))
secs = sorted(list(df.bnf_sec.unique()))
for sec in secs:
    df_sec = df.loc[df.bnf_sec ==  sec]
    fig, axes = plt.subplots(1, len(years), figsize=(3 * len(years), 3))
    ax_itr = iter(axes)
    print(df_sec)
    for year in years:
        print('\n\n',year)
        slc = df_sec.loc[year]
        tot = slc.sum().at['items']
        srted = slc.sort_values('items')

        #for k in range(5,51,5):
        for k in range(1,6,2):
            top = srted.tail(k).sum()['items']
            print(f'k = {k}, {top / tot}')

        items = slc.sort_values('items', ascending=False).reset_index()['items'].head(500)
        items.plot(ax=next(ax_itr), title=str(year))

    #plt.show()


