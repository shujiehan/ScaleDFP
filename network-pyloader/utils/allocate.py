import sys
sys.path.append("..")
import config
import numpy as np
import pandas as pd
from random_poisson import RandomPoisson
import time


class Allocate:
    '''
    Allocate positive and negative samples from each data source to base learners.
    '''

    def __init__(self, features=None, collector=None, random_seed=None, frac=None, bl_allocation=False,
                 num_trees=None, down_ratio=None, lamb_p=None, lamb_n=None):
        self.collector = collector
        self.frac = frac
        self.bl_allocation = bl_allocation
        self.num_trees = num_trees
        self.add_columns = None
        if self.bl_allocation:
            # enable training data allocation
            self.add_columns = ['weight_%d'%i for i in range(self.num_trees)]
        self.features = ['date', 'serial_number', 'failure'] + features
        # downsampling ratio in training data allocation
        self.down_ratio = down_ratio
        self.updated_down_ratio = self.down_ratio
        self.imbalance_ratio = -1
        self.lamb_p = lamb_p
        self.lamb_n = lamb_n
        self.random_poisson = RandomPoisson(random_seed)
        if random_seed is not None:
            np.random.seed(random_seed)

        self.sum_time_random_down = 0
        self.sum_time_allocation = 0

    def poisson_sampling(self, group, label):
        group_size = group.shape[0]
        if label == 'c1':
            weights = self.random_poisson.multiplePoisson(self.lamb_p, group_size * self.num_trees)
        else:
            weights = self.random_poisson.multiplePoisson(self.lamb_n/self.updated_down_ratio, group_size * self.num_trees)
        weights = np.array(weights).reshape(group_size, self.num_trees)
        df_weights = pd.DataFrame(weights, index=group.index, columns=self.add_columns)
        return df_weights

    def send_downsampling_random(self, name, df, output):
        if self.frac < 1:
            t1 = time.time()
            df_pos = df[df['failure'] == 'c1']
            df_neg = df[df['failure'] == 'c0']
            df_neg = df_neg.sample(frac=self.frac, random_state=1)
            df = pd.concat([df_pos, df_neg], axis=0).sort_index()
            self.sum_time_random_down += (time.time() - t1)

        # training data allocation
        t1 = time.time()
        if self.bl_allocation:
            # 1. send positive and negative sample counts to coordinator
            num_positive_samples = df[df['failure'] == 'c1'].shape[0]
            num_negative_samples = df[df['failure'] == 'c0'].shape[0]
            self.collector.upload_local_samples_count(num_positive_samples, num_negative_samples)

            if self.num_trees is not None:
                # The initial imbalance ratio is computed based on each data collector
                if self.imbalance_ratio == -1:
                    if df[df['failure'] == 'c1'].shape[0] == 0:
                        self.imbalance_ratio = 0.001
                    else:
                        self.imbalance_ratio = df[df['failure'] == 'c1'].shape[0] / df[df['failure'] == 'c0'].shape[0]
                #print("imbalance ratio", self.imbalance_ratio)
                # Then we update the imbalance ratio based on the global number of positive and negative samples
                self.updated_down_ratio = self.down_ratio / self.imbalance_ratio
                # compute poisson sampling for each sample
                weight_list = []
                for k, group in df.groupby((df['failure'].shift() != df['failure']).cumsum()):
                    weight = self.poisson_sampling(group, group['failure'].values[0])
                    weight_list.append(weight)
                df_weight = pd.concat(weight_list, axis=0)
                df = pd.concat([df, df_weight], axis=1)
        else:
            # disable training data allocation
            pass
        # 2. send the corresponding dataframe to receivers
        self.collector.upload_samples(filename=name, chunk=df, output=output, allocation=self.bl_allocation, features=self.features)
        if self.bl_allocation:
            # 3. receive total positive and negative sample counts from coordinator
            num_positive_samples, num_negative_samples = self.collector.get_global_samples_count()
            if num_positive_samples == 0:
                self.imbalance_ratio = 0.001
            else:
                self.imbalance_ratio =  num_positive_samples / num_negative_samples
        self.sum_time_allocation += (time.time() - t1)

