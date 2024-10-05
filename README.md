# ScaleDFP

ScaleDFP is a general framework for scaling disk failure prediction with the number of data sources via multi-source stream mining. It is designed based on three techniques: near-data-preprocessing, random downsampling, and training data allocation. 

We prototyped SCALEDFP in Python, comprising data collectors, a coordinator, and receivers with ∼550 LoC. Among different components, we implement network communications using Remote Procedure Calls (RPCs) via [gRPC](https://grpc.io/). 

- For data collectors, we realize near-data preprocessing based on the preprocessing workflow in [StreamDFP](https://github.com/shujiehan/StreamDFP) and integrate random downsampling. After random downsampling, each data collector sends its local counts of positive and negative samples to the coordinator. 

- The coordinator then returns the ratio of the total number of positive samples to negative samples to each data collector. We also implement the customized Poisson sampling on data collectors. Due to Python’s inherent performance limitations in for-loops for computing weights for each sample across all base learners, we optimize the Poisson sampling computation using [pybind11](https://github.com/pybind/pybind11). 

- Additionally, data collectors transmit training data to receivers via RPCs. Receivers ensure receipt of training data from all data collectors and sort samples by their timestamps before writing them into the local file system. Samples are read from the local file system and fed into the ensemble learning algorithms (i.e., BA and ARF). We realize the ensemble learning algorithms based on StreamDFP and replace its original Poisson sampling approach by our training data allocation approach.

## Prerequisite

- Preprocessing:
  - Python>=3.7
  - Python library: `pip install -r requirements.txt`
  - [Random poisson library](https://github.com/shujiehan/random_poisson): refer to README.md in this library for installing
- Training:
  - Java: jdk-1.8.0

## Dataset

We use the following 3 disk models in public dataset [Backblaze](https://www.backblaze.com/b2/hard-drive-test-data.html):

- Seagate ST3000DM001
- Seagate ST4000DM000
- HGST HMS5C4040BLE640

In addition, we use the following 3 SSD models in [public datasets at Alibaba](https://github.com/alibaba-edu/dcbrain/tree/master/ssd_smart_logs):

- MA1
- MB1
- MC1

You can also use other disk models for testing.

## Usage

### Prepare data

- As ScaleDFP is designed for scaling disk failure prediction, we first need to split the whole dataset into several partitions.
- Go to `data_preparation` and see its `README.md`.
- After preparing the data, you will have multiple data partitions, each of which represents a data source. Also, the data partitions are stored under `[your prefix path]/bb_raw_[n_split]p/collector*` for Backblaze dataset or `[your prefix path]/ali_raw_[n_split]p/collector*` for Alibaba dataset.

### Preprocessing

#### Test the system performance

Please first go to `network-pyloader/` :

- Edit your `config.py` to configure the following parameters:
  
  - `NUM_COLLECTORS`: the number of data collectors.
  
  - `RECEIVER_MODEL_MAP`: the table of mapping weight columns to receivers.
  
  - `RECEIVER_ADDR_MAP`: the table of mapping receiver names to receiver ip addresses.
  
  - `COLLECTOR_ADDRESS_MAP`: the table of mapping collector names to collector ip addresses.
  
  - `WRITE`: indicate if you need to write the training data into the local file systems of receivers. When testing the throughput, we set `WRITE=False`. When testing the accuracy, we set `WRITE=True`.
  
  - If you run ScaleDFP on **multiple** machines, make sure that each machine has the same `config.py`.

- run coordinator in one process by `python simple_coord.py`

- run a receiver in one process by `python simple_receiver.py [receiver_name]`
  
  - e.g., `python simple_receiver.py receiver0`
  
  - check the receiver names in config.py

- run multiple collectors
  
  - To run multiple data collectors simultaneously, please refer to the script `run.sh`
    
    - In `run.sh`, we run each collector with a daemon by `run_st4_multi.sh`
    
    - `run_st4_multi.sh` is an example for running the disk model Seagate ST4000DM000. You may replace it with any disk model that we would like to run.
  
  - In `run_st4_multi.sh`, to run each collector, we use the following command:

```
python run_random_down.py
-s <start_date> [--start_date <start_date>]
-a <label_days> [--label_days <label_days>]
-p <path_dataset> [--path <path_dataset>]
-r <train_data_path> [--train_path <train_data_path>]
-e <test_data_path> [--test_path <test_data_path>]
-c <path_features> [--path_features <path_features>]
-o <option> [--option <option>] (4: enable labeling; 
                                 7: enable training data allocation)
-C <collector_id> [--collector_id <collector_id>]
-P <lamb_p> [--lamb_p <lamb_p>]
-N <lamb_n> [--lamb_n <lamb_n>]
-x <frac> [--frac <frac>]
-T <test> [--test <test>] ("performance" or "accuracy")
```

For more details, please run `python run_random_down.py -h` or refer to an example script `run_st4_multi.sh`.

#### Test the baseline of system performance

- We use the client and server in `file_transfer/` to simulate data collection from multiple data collectors.
  
  - We implement a simple file transmission via gRPC.
  
  - You may refer to `client.py` (i.e., data collectos) and `server.py` to check out the details.
  
  - You can use `config.py` to set the server ip address, data path, and the number of data collectors.
  
  - You may refer to the script `start.py` for starting multiple clients simultaneously.

- We then test the performance of preprocessing by `pyloader/`, which is inherited from StreamDFP as the baseline.

#### Test the accuracy

- Set `-T accuracy` when running `run_random_down.py`
  
  - `python run_random_down.py -T accuracy`.
  
  - When testing the accuracy, we need to inject the zero-weight samples to change detectors. As the change detectors can be deployed on different servers from the base learners, we leave the implementations of distributed learning in the future work.

- As the training in StreamDFP does not support multiple data sources as input, we fix one receiver to receive the preprocessed data from all data collectors. 

### Training and prediction in Java

Please go back to `ScaleDFP/`:

```
java -cp simulate/target/simulate-2024.01.0-SNAPSHOT.jar:moa/target/moa-2024.01.0-SNAPSHOT.jar simulate.Simulate
-s <start_date> 
-p <train_data_path>
-t <test_data_path>
-g [enable regression task]
-a <classifier>
-L <label_days>
-D <down_sample_ratio>
```

For more details, please refer to an example script `run_st4_example.sh`.

The above process is used to generate the training data. 

For the test data, please use `pyloader/`, which is the original data preprocessing in StreamDFP, to generate the test data. You may refer to StreamDFP to check out the usage, which is similar to ScaleDFP.

### Contact

Please email to Shujie Han (shujiehan@nwpu.edu.cn) if you have any questions.
