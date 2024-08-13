import pandas as pd
import os

def re_partition(load_dt):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/tmp/sparkdata/load_dt={load_dt}'
    write_path = f'{home_dir}/data/movie/repartition/'
    df = pd.read_parquet(read_path)
    df['load_dt'] = load_dt
    df.to_parquet("~/data/movie/repartition/", partition_cols=['load_dt','multiMovieYn', 'repNationCd'])


