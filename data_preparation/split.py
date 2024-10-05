import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from joblib import Parallel, delayed


def bb_pre_walk(prefix, start_date, num_dates, date_format):
    end_date = datetime.strptime(start_date, date_format) + timedelta(days=num_dates)
    date_list = pd.date_range(start_date, end_date, closed='left', freq='D')
    frames = []
    for date in date_list.strftime(date_format).to_list():
        print(date)
        df = pd.read_csv(f"{prefix}/backblaze/{date}.csv")
        frames.append(df.loc[:, ['serial_number', 'model']])

    location = pd.concat(frames, ignore_index=True)
    location.drop_duplicates()
    location.to_csv(f"{prefix}/backblaze/location_info.csv", index=0)


def bb_random_split_each_file(prefix, n_splits, start_date, num_dates, date_format):
    end_date = datetime.strptime(start_date, date_format) + timedelta(days=num_dates)
    date_list = pd.date_range(start_date, end_date, closed='left', freq='D')
    df_loc = pd.read_csv(f"{prefix}/backblaze/location_info.csv")
    disks_sn = np.array(df_loc.serial_number.unique())
    sn_index = np.random.permutation(len(disks_sn))
    sn_index_list = np.array_split(sn_index, n_splits)
    sn_list = [list(disks_sn[split_idx]) for split_idx in sn_index_list]

    for date in date_list.strftime(date_format).to_list():
        print(date)
        df = pd.read_csv(f"{prefix}/backblaze/{date}.csv")
        Parallel(n_jobs=4)(delayed(parallel_write_random)(prefix, "bb", df, idx, split, date, n_splits)
                                  for idx, split in enumerate(sn_list))


def random_split_each_file(prefix, n_splits, start_date, num_dates, date_format):
    end_date = datetime.strptime(start_date, date_format) + timedelta(days=num_dates)
    date_list = pd.date_range(start_date, end_date, closed='left', freq='D')
    df_loc = pd.read_csv(f"{prefix}/alibaba_ssd/location_info_of_ssd.csv")
    df_loc['sn'] = df_loc['model'] + '/' + df_loc['disk_id'].astype(str)
    disks_sn = np.array(df_loc.sn.unique())
    sn_index = np.random.permutation(len(disks_sn))
    sn_index_list = np.array_split(sn_index, n_splits)
    sn_list = [list(disks_sn[split_idx]) for split_idx in sn_index_list]

    for date in date_list.strftime(date_format).to_list():
        print(date)
        df = pd.read_csv(f"{prefix}/alibaba_ssd/data/{date}.csv")
        df['sn'] = df['model'] + '/' + df['disk_id'].astype(str)
        Parallel(n_jobs=4)(delayed(parallel_write_random)(prefix, "ali", df, idx, split, date, n_splits)
                                  for idx, split in enumerate(sn_list))


def parallel_write_random(prefix, output_prefix, df, idx, split, date, n_splits):
    df_tmp = pd.DataFrame()
    if output_prefix == 'bb':
        df_tmp = df[df['serial_number'].isin(split)]
    elif output_prefix == 'ali':
        #print(df_tmp.shape)
        df_tmp = df[df['sn'].isin(split)]
        df_tmp = df_tmp.drop(['sn'], axis=1)
    path = f"{prefix}/{output_prefix}_raw_{str(n_splits)}p/collector{str(idx + 1)}"
    if not os.path.exists(path):
        os.makedirs(path)
    df_tmp.to_csv(f"{prefix}/{output_prefix}_raw_{str(n_splits)}p/collector{str(idx + 1)}/{date}.csv", index=False)


if __name__ == '__main__':
    n_splits = int(sys.argv[1])
    start_date = sys.argv[2]
    num_dates = int(sys.argv[3])
    prefix = sys.argv[4]
    datasets = sys.argv[5]
    if datasets == "backblaze":
        bb_pre_walk(prefix, start_date, num_dates, "%Y-%m-%d")
        bb_random_split_each_file(prefix, n_splits, start_date, num_dates, "%Y-%m-%d")
    elif datasets == "alibaba":
        random_split_each_file(prefix, n_splits, start_date, num_dates, "%Y%m%d")
    else:
        print("datasets should be \"backblaze\" or \"alibaba\"")
        sys.exit(1)
