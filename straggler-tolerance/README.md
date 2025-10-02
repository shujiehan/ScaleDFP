### Dataset

We use [Alibaba's Machine-Learning as-a-Service (MLaaS) trace](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-gpu-v2020) for training the prediction model for long queuing delays.

### Prerequisite

- Download the MLaaS trace and save it into `./prediction/data` directory.

- Decompress the MLaaS trace into `.csv` files in `./prediction/data`.

- Compile the cython file to accelerate the feature generation. 
  
  - Go to `./prediction/utils`.
  
  - Run `python setup.py build` , which compiles the cython file under `cython_resource_features/resource_features_cython.pyx` locally.

### Preprocessing

- Go to `./prediction` directory.

- Run `python merge_dataset.py` to merge jobs, tasks, and instances into one file. The merged data will be saved into `./prediction/data/instance_merged.csv`.

- Run `python preprocess.py` to clean the data. The data after cleaning will be saved into `./prediction/data/instance_preprocessed.csv`.

### Prediction of long queuing delays

- Run `python rf_classify.py` to predict long queuing delays. The prediction results will be saved into `./prediction/results/`.

### Simulation of straggler-aware scheduler

#### 1. Throttle the network bandwidth

- Go to `./scheduler/migration` directory.

- Generate message protocols by `./codegen.sh`.

- Throttle the network bandwidth
  
  - You can throttle the network bandwidth to 8Gpbs (as intra-rack network bandwidth) by `sudo tc qdisc add dev eth0 root tbf rate 8gbit burst 16mb latency 10ms`.
  
  - You can throttle the network bandwidth to 1Gpbs (as cross-rack network bandwidth) by `sudo tc qdisc add dev eth0 root tbf rate 1gbit burst 2mb latency 10ms`.

#### 2. Migrating data collectors

- Test the performance of migrating data collectors within a rack or across racks.
  
  - Test by yourself:
    
    - Run `python server.py` on one terminal.
    
    - Run `python migrate.py` on another terminal.
  
  - **OR you can directly use our provided results**: We provide the results of migrating data collectors within a rack or across racks in `results_migration.tar.gz`, which includes
    
    - various numbers of data collectors (one to 128 data collectors),
    
    - results with the suffix of `_1gbps`, simulating the cross-rack bandwidth as 1Gbps, and
    
    - results with the suffix of `_8gbps`, simulating the intra-rack bandwidth as 8Gbps.

#### 3. Forwarding raw data

- Test the performance of forwarding raw data across racks.
  
  - Test by yourself:
    
    - Run `python server.py` on one terminal.
    
    - Run `python forward.py` on another terminal.
  
  - **OR you can directly use our provided results**: We provide the results of forwarding raw data across racks in`results_forward.tar.gz`, which includes
    
    - various numbers of data collectors (one to 128 data collectors).

#### 4. Evaluate the synchronization overhead

- Evaluate the synchronization overhead of straggler-aware scheduler.
  
  - The file `./prediction/data/pai_machine_spec_rack.csv` includes the random topology of racks and machines.
  - Go to `./scheduler` directory.
  - Run `./run_straggler_manager.sh`, which will run `straggler_simulator.py` and compare the straggler-aware scheduler with two random baselines.
