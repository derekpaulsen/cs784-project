import pandas as pd
from numpy import int32, float32, int16
from joblib import Parallel, delayed
from zipfile import ZipFile 
from pathlib import Path
import os
os.system("taskset -p 0xfffffffff %d" % os.getpid())


#import psutil
#p = psutil.Process()
#
## reset affinity against all CPUs
#all_cpus = list(range(psutil.cpu_count()))
#p.cpu_affinity(all_cpus)

# total = 3
# males = 4
# female = 5

pool = Parallel(n_jobs=-1, timeout=60*100)



msoa_map = {}

df = pd.read_csv('/data/Postcode_to_Output_Area_to_Lower_Layer_Super_Output_Area_to_Middle_Layer_Super_Output_Area_to_Local_Authority_District_February_2018_Lookup_in_the_UK.csv')


msoa_map.update(
    dict(zip(df.pcds, df.msoa11cd))
)
msoa_map.update(
    dict(zip(df.pcd7, df.msoa11cd))
)
msoa_map.update(
    dict(zip(df.pcd8, df.msoa11cd))
)

del df

 

def get_id_to_postcode(zf):
    addrs = pd.read_csv(zf.open(zf.namelist()[0]), header=None)
    return dict(zip(addrs[1], addrs[7].apply(lambda x : x.strip())))
    
def process_zip_file(fname):
    zf = ZipFile(fname)
    id_to_oc = get_id_to_postcode(zf)

    data = pd.read_csv(zf.open(zf.namelist()[2]))
    data.rename(str.strip, inplace=True, axis=1)


    clean_data = pd.DataFrame({
        'msoa' : data.PRACTICE.apply(lambda x : msoa_map.get(id_to_oc[x])),
        'bnf_sec' : data['BNF CODE'].apply(lambda x : int(x[:2])).astype(int16),
        'bnf_sub_sec' : data['BNF CODE'].apply(lambda x : int(x[2:4])).astype(int16),
        'year' : data['PERIOD'].apply(lambda x : x // 100).astype(int16),
        'quantity' : data.QUANTITY.astype(int32),
        'items' : data.ITEMS.astype(int32),
        'net_ingredient_cost' : data.NIC.astype(float32),
        'act_cost' : data['ACT COST'].astype(float32)
    },
    )

    print(clean_data.head())
    print(f'total length {len(clean_data)}')
    print('NULL counts')
    for c in clean_data.columns:
        nn = clean_data[c].isnull().sum()
        print(f'{c} : {nn}')

    return clean_data

    
def process(f):
    os.system("taskset -p 0xfffffffff %d" % os.getpid())
    try:
        df = process_zip_file('/data/zip_files/' + f.name)
    except Exception as e:
        print(f'processing {f.name} FAILED')
        print(e)
    return df

def main():

    path = Path('/data/zip_files/')
    files = (x for x in path.iterdir())
    dfs = pool(delayed(process)(f) for f in files)

    agg = pd.concat(dfs, ignore_index=True, copy=False)

    agg.to_csv('/data/prescribe_msoa.csv', index=False)


        


if __name__ == '__main__':
    main()
