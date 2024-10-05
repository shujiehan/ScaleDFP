### Data Preparation

Our goal is to scale out disk failure prediction by formulating it as a multi-source stream mining problem. We conceptualize disk logs as continuously collected data streams originating from multiple sources, with the total number of data sources denoted by `n`. Specifically, we consider a typical data center composed of multiple racks, each hosting several machines connected via a top-of-rack switch. Each machine is equipped with multiple disks. We regard each rack as a data source that generates a data stream consisting of disk logs from the disks attached to the rack.

This directory is used to split the whole dataset of Backblaze or Alibaba into several partitions. Each partition represents a data source. As the whole dataset is organized by dates, we divide each file of one day into the specific number of splits. To fix a set of disks for a specific data source, we first obtain the global view of dataset by going through the whole dataset to get the locations of disks, which is provided by Alibaba dataset but not provided by Backblaze dataset.


#### Preprequisite

`pip install -r requirements.txt`


#### Usage

`python split.py [n_split] [start_date] [num_dates] [prefix] [datasets]`

- `n_split`: the number of partitions (i.e., data sources).

- `start_date`: the start date of data that you use, e.g., 2015-01-01 for Backblaze or 2018010 for Alibaba

- `num_dates`: the number of days (i.e., files) that you want to split, e.g., 460 days.

- `prefix`: the prefix path of your dataset. Suppose that you store Backblaze datasets under `~/data/backblaze/*.csv`. `prefix` is `~/data`.

- `datasets`: indicate which datasets you use. It should be  `backblaze` or `alibaba`.



The output data is stored under `[prefix]/bb_raw_[n_split]p/` for Backblaze dataset or `[prefix]/ali_raw_[n_split]p/` for Alibaba dataset.
