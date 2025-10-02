#Copyright 2025 Northwestern Polytechnical University
#@author Shujie Han
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import numpy as np
import pandas as pd
import random
import sys

DATA_PATH = "../prediction/data"
PREDICTION_RESULT_PATH = "../prediction/results"
SCHEDULER_RESULT_PATH = "./results"

def read_data(num_collectors):
    df_machines = pd.read_csv(f"{DATA_PATH}/pai_machine_spec_rack.csv")
    rack_list = df_machines['rack'].unique().tolist()

    waiting_time_predictions_fname = f"{PREDICTION_RESULT_PATH}/predictions_rf_classification_stat_features369_waiting10_threshold0.5_down1_tree200_resample.csv"
    df_waiting_time = pd.read_csv(waiting_time_predictions_fname)
    df_waiting_time = df_waiting_time.rename(columns={'submit_time': 'job_start_time', 'waiting_time': 'actual_waiting_time'})
    df_waiting_time['job_start_time'] = pd.to_datetime(df_waiting_time['job_start_time'])
    df_waiting_time = df_waiting_time.sort_values(['job_start_time'])

    # read migration time and forwarding time
    df_cross_rack_migration = pd.read_csv(f"migration/results_migration/{num_collectors}p_1gbps.csv")
    df_cross_rack_migration['date'] = pd.to_datetime(df_cross_rack_migration['date'])
    df_intra_rack_migration = pd.read_csv(f"migration/results_migration/{num_collectors}p_8gbps.csv")
    df_intra_rack_migration['date'] = pd.to_datetime(df_intra_rack_migration['date'])
    # read forwarding time across racks
    df_forward_time = pd.read_csv(f"migration/results_forward/{num_collectors}p.csv")
    df_forward_time['date'] = pd.to_datetime(df_forward_time['date'])
    start_disk_date = pd.Timestamp('2018-03-04')
    return df_machines, df_waiting_time, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, rack_list, start_disk_date

def assign_collectors_to_machines(rack_list, df_machines):
    sampled_racks = random.sample(rack_list, num_collectors)
    sampled_nodes = []
    for rack in sampled_racks:
        sampled_nodes.append(random.sample(df_machines[df_machines['rack'] == rack]['machine'].values.tolist(), 1)[0])
    collector_node_map = {f"collector{idx + 1}": node for idx, node in enumerate(sampled_nodes)}
    selected_indices = df_machines[df_machines['machine'].isin(sampled_nodes)].index
    df_machines['collectors'] = np.nan
    df_machines.loc[selected_indices, 'collectors'] = list(collector_node_map.keys())
    return df_machines

def merge_collectors_and_time(df_collectors_machines, df_waiting_time, current_time_point):
    current_predictions_df = df_waiting_time[df_waiting_time['job_start_time'] == current_time_point]
    df_collectors_time = pd.merge(df_collectors_machines, current_predictions_df, on='machine', how='left')
    df_collectors_time[['actual_waiting_time']] = df_collectors_time[['actual_waiting_time']].fillna(0)
    df_collectors_time[['predicted_prob_long_waiting_time']] = df_collectors_time[['predicted_prob_long_waiting_time']].fillna(-1)
    print(f"actual_waiting_time=\n {df_collectors_time[~df_collectors_time['collectors'].isna()]}")
    return df_collectors_time

def migrate_collectors(df_collectors_time, new_assignments, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, current_disk_date):
    # Update the main map with all new assignments at the end
    df_collectors_time['migrated_collector'] = df_collectors_time['machine'].map(new_assignments)
    print(f"df_collectors_time=\n {df_collectors_time[(~df_collectors_time['collectors'].isna()) | (~df_collectors_time['migrated_collector'].isna())][['rack', 'machine', 'collectors', 'migrated_collector', 'predicted_prob_long_waiting_time', 'actual_waiting_time']]}")
    # check if a collector migrates to a machine in cross racks compared to original rack
    df_org = df_collectors_time[~df_collectors_time['collectors'].isna()][['rack', 'collectors']].rename(columns={'rack': 'rack_org'})
    df_migrate = df_collectors_time[~df_collectors_time['migrated_collector'].isna()][['rack', 'migrated_collector']].rename(columns={'migrated_collector': 'collectors', 'rack': 'new_rack'})
    df_migrate = pd.merge(df_migrate, df_org, how='left', on='collectors')
    df_migrate['rack_type'] = np.where(df_migrate['rack_org'] == df_migrate['new_rack'], 'intra', 'cross')
    print(f"migration data = \n {df_migrate}")
    # if all intra-rack migration
    intra_rack_collectors = df_migrate[df_migrate['rack_type'] == 'intra']['collectors'].values.tolist()
    max_intra_rack_migration_time = 0
    if intra_rack_collectors:
        print(f"df_intra_rack_migration = \n{df_intra_rack_migration[(df_intra_rack_migration['collectors'].isin(intra_rack_collectors)) & (df_intra_rack_migration['date'] == current_disk_date)]}")
        max_intra_rack_migration_time = df_intra_rack_migration[(df_intra_rack_migration['collectors'].isin(intra_rack_collectors)) & (df_intra_rack_migration['date'] == current_disk_date)]['time'].max()
    # print(f"intra_rack_collectors = {intra_rack_collectors}")

    cross_rack_collectors = df_migrate[df_migrate['rack_type'] == 'cross']['collectors'].values.tolist()
    sum_cross_rack_migration_time = 0
    if cross_rack_collectors:
        print(f"cross_rack = \n {df_migrate}")
        g = df_migrate.groupby(['new_rack'])['collectors'].count().reset_index().rename(columns={'collectors': 'count'}).sort_values('count', ascending=False)
        congested_rack = g.head(1)['new_rack'].values.tolist()
        cross_rack_collectors = df_migrate[df_migrate['new_rack'].isin(congested_rack)]['collectors'].values.tolist()
        sum_cross_rack_migration_time = df_cross_rack_migration[(df_cross_rack_migration['collectors'].isin(cross_rack_collectors)) & (df_cross_rack_migration['date'] == current_disk_date)]['time'].sum()
        # add migration time of data collectors across racks or within rack; if across racks, add forwarding time of next-day data
        sum_cross_rack_forward_time = df_forward_time[(df_forward_time['collectors'].isin(cross_rack_collectors)) & (df_forward_time['date'] == current_disk_date)]['time'].sum()
        sum_cross_rack_migration_time += sum_cross_rack_forward_time

    #print(f"before updated=\n{df_collectors_time[(~df_collectors_time['collectors'].isna()) | (~df_collectors_time['migrated_collector'].isna())][['rack', 'machine', 'collectors', 'migrated_collector']]}")
    # update original df_collectors_time
    original_slow_machines = {k: v for k, v in df_collectors_time[df_collectors_time['collectors'].isin(new_assignments.values())]['machine'].to_dict().items()}
    df_collectors_time.loc[df_collectors_time['machine'].isin(original_slow_machines.values()), 'collectors'] = np.nan
    df_collectors_time.loc[df_collectors_time['migrated_collector'].notna(), 'collectors'] = df_collectors_time['migrated_collector']
    #print(f"after updated=\n{df_collectors_time[~df_collectors_time['collectors'].isna()][['rack', 'machine', 'collectors', 'migrated_collector']]}")
    df_collectors_time.drop(columns='migrated_collector', inplace=True)
    print(f"max_intra_rack_migration_time = {max_intra_rack_migration_time}")
    print(f"sum_cross_rack_migration_time = {sum_cross_rack_migration_time}")
    return max(max_intra_rack_migration_time, sum_cross_rack_migration_time)

def random_schedule(df_collectors_machines, df_waiting_time, start_time_point, end_time_point):
    current_time_point = start_time_point
    accumulated_waiting_time = 0
    while current_time_point <= end_time_point:
        print(f"\ncurrent_time_point={current_time_point}")
        df_collectors_time = merge_collectors_and_time(df_collectors_machines, df_waiting_time, current_time_point)
        print(f"df_collectors_time=\n {df_collectors_time[~df_collectors_time['collectors'].isna()][['collectors', 'rack', 'actual_waiting_time']]}")
        max_actual_waiting_time = df_collectors_time[~df_collectors_time['collectors'].isna()]['actual_waiting_time'].max()
        print(f"max_actual_waiting_time = {max_actual_waiting_time}")
        accumulated_waiting_time += max_actual_waiting_time
        print(f"accumulated_waiting_time={accumulated_waiting_time}")
        current_time_point += pd.Timedelta('24H')
    return accumulated_waiting_time

def main_for_random_schedule(num_collectors, seed):
    df_machines, df_waiting_time, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, rack_list, start_disk_date = read_data(num_collectors)
    accumulated_waiting_times = []
    for start_hour in range(24):
        random.seed(seed)
        start_time_point = pd.Timestamp(f"1970-02-17 {start_hour}:00:00+08:00")
        end_time_point = pd.Timestamp("1970-03-15 23:00:00+08:00")
        df_machines = assign_collectors_to_machines(rack_list, df_machines)
        print(f"df_machine = \n{df_machines[~df_machines['collectors'].isna()][['rack', 'machine', 'collectors']]}")
        accumulated_waiting_times.append(random_schedule(df_machines, df_waiting_time, start_time_point, end_time_point))
    hours = [x for x in range(24)]
    res_df = pd.DataFrame({'hour': hours, 'accumulated_waiting_time': accumulated_waiting_times})
    res_df.to_csv(f'{SCHEDULER_RESULT_PATH}/random_schedule_c{num_collectors}_{seed}.csv', index=False)

def random_timeout_migration(df_collectors_time, df_slow_collectors, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, current_disk_date):
    all_candidate_nodes = df_collectors_time[df_collectors_time['collectors'].isna()]['machine'].tolist()
    new_nodes = random.sample(all_candidate_nodes, len(df_slow_collectors))
    new_assignments = {}
    for idx, collector in enumerate(df_slow_collectors['collectors'].tolist()):
        new_assignments[new_nodes[idx]] = collector
    migration_time = migrate_collectors(df_collectors_time, new_assignments, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, current_disk_date)
    return df_collectors_time, migration_time


def random_timeout_schedule(df_collectors_machines, df_waiting_time, df_cross_rack_migration, df_intra_rack_migration, df_forward_time,
                            start_time_point, end_time_point, start_disk_date, timeout_threshold, timeout_migrating_threshold):
    current_time_point = start_time_point
    current_disk_date = start_disk_date
    accumulated_waiting_time = 0
    while current_time_point <= end_time_point:
        print(f"\ncurrent_time_point={current_time_point}")
        df_collectors_time = merge_collectors_and_time(df_collectors_machines, df_waiting_time, current_time_point)
        max_actual_waiting_time = df_collectors_time[~df_collectors_time['collectors'].isna()]['actual_waiting_time'].max()

        timeout_migrating_counter = 0
        while max_actual_waiting_time > timeout_threshold and timeout_migrating_counter < timeout_migrating_threshold:
            # Optimized with list comprehension
            df_slow_collectors = df_collectors_time[(~df_collectors_time['collectors'].isna()) & (df_collectors_time['actual_waiting_time'] > timeout_threshold)]
            print("======Timeout migrating======")
            df_collectors_time, migration_time = random_timeout_migration(df_collectors_time, df_slow_collectors, df_cross_rack_migration,
                                                                          df_intra_rack_migration, df_forward_time, current_disk_date)
            accumulated_waiting_time += migration_time
            timeout_migrating_counter += 1
            max_actual_waiting_time = df_collectors_time[~df_collectors_time['collectors'].isna()]['actual_waiting_time'].max()
            print(f"timeout migrating counter = {timeout_migrating_counter}")

        accumulated_waiting_time += (timeout_migrating_counter * timeout_threshold + max_actual_waiting_time)
        print(f"accumulated_waiting_time={accumulated_waiting_time}")
        # updated mapping between collectors and machines
        df_collectors_machines = df_collectors_time[['machine', 'gpu_type', 'cap_cpu', 'cap_mem', 'cap_gpu', 'rack', 'collectors']]
        current_time_point += pd.Timedelta('24H')
        current_disk_date += pd.Timedelta('24H')
    return accumulated_waiting_time

def main_for_random_timeout_schedule(num_collectors, seed):
    df_machines, df_waiting_time, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, rack_list, start_disk_date = read_data(num_collectors)
    accumulated_waiting_times = []
    timeout_threshold = 10
    timeout_migrating_threshold = 3
    for start_hour in range(24):
        random.seed(seed)
        start_time_point = pd.Timestamp(f"1970-02-17 {start_hour}:00:00+08:00")
        end_time_point = pd.Timestamp("1970-03-15 23:00:00+08:00")
        df_machines = assign_collectors_to_machines(rack_list, df_machines)
        print(f"df_machine = \n{df_machines[~df_machines['collectors'].isna()][['rack', 'machine', 'collectors']]}")
        accumulated_waiting_times.append(random_timeout_schedule(df_machines, df_waiting_time, df_cross_rack_migration, df_intra_rack_migration, df_forward_time,
                                                              start_time_point, end_time_point, start_disk_date,
                                                              timeout_threshold, timeout_migrating_threshold))
    hours = [x for x in range(24)]
    res_df = pd.DataFrame({'hour': hours, 'accumulated_waiting_time': accumulated_waiting_times})
    res_df.to_csv(f'{SCHEDULER_RESULT_PATH}/random_timeout_schedule_random_migration_forward_c{num_collectors}_timeout{timeout_threshold}_migrating{timeout_migrating_threshold}_{seed}.csv', index=False)


def intelligent_migration_hybrid(df_collectors_time, df_cross_rack_migration, df_intra_rack_migration,
                                 df_forward_time, df_slow_collectors, current_disk_date, migration_prob, seed, migrating_method='sorting'):
    new_assignments = {}  # To store the new collector -> node mappings
    if migrating_method == 'sorting':
        #df_tmp = df_waiting_time[df_waiting_time['job_start_time'] == current_time_point].sort_values(by='predicted_prob_long_waiting_time')
        for rack, group in df_slow_collectors.groupby('rack'):
            num_to_migrate = len(group)
            current_nodes_in_rack = group['machine'].tolist()
            # 1. Get all candidate nodes for this rack ONCE
            df_candidate_nodes_in_this_rack = df_collectors_time[(df_collectors_time['rack'] == rack) & (df_collectors_time['collectors'].isna())]
            if len(df_candidate_nodes_in_this_rack) == 0:
                continue  # Skip if no other nodes are available in the rack
            # 2. Split candidates into those with and without predictions ONCE
            #df_nodes_with_predictions = df_candidate_nodes_in_this_rack[df_candidate_nodes_in_this_rack['predicted_prob_long_waiting_time'] != -1].sort_values(by='predicted_prob_long_waiting_time')
            df_nodes_with_predictions = df_candidate_nodes_in_this_rack[(df_candidate_nodes_in_this_rack['predicted_prob_long_waiting_time'] != -1) &
                                                                        (df_candidate_nodes_in_this_rack['predicted_prob_long_waiting_time'] < migration_prob)].sort_values(by='predicted_prob_long_waiting_time')
            print(f"df_nodes_with_predictions = {df_nodes_with_predictions[['rack', 'machine', 'predicted_prob_long_waiting_time', 'actual_waiting_time']]}")
            nodes_with_predictions = df_nodes_with_predictions['machine'].tolist()
            nodes_without_predictions = df_candidate_nodes_in_this_rack[df_candidate_nodes_in_this_rack['predicted_prob_long_waiting_time'] == -1]['machine'].tolist()

            # 3. Build the final candidate pool for this rack ONCE
            num_candidates_with_predicted_values = 10
            candidates_with_predicted_values = df_nodes_with_predictions.head(num_candidates_with_predicted_values)['machine'].tolist()
            # Calculate proportional number of candidates without predictions
            num_candidates_without_predicted_values = 0
            if len(df_nodes_with_predictions) > 0:
                ratio = len(nodes_without_predictions) / len(nodes_with_predictions)
                num_candidates_without_predicted_values = int(ratio * len(candidates_with_predicted_values))

            candidates_without_predicted_values = random.sample(nodes_without_predictions, min(len(nodes_without_predictions), num_candidates_without_predicted_values))
            final_candidate_pool = candidates_with_predicted_values + candidates_without_predicted_values
            # 4. Sample and assign new nodes for all collectors in this rack's group
            if not final_candidate_pool:
                continue  # Skip if no candidates found
            if len(final_candidate_pool) < num_to_migrate:
                # If not enough new nodes, assign what we have
                new_nodes_for_group = random.sample(final_candidate_pool, len(final_candidate_pool))
            else:
                new_nodes_for_group = random.sample(final_candidate_pool, num_to_migrate)

            for i, collector in enumerate(group['collectors']):
                if i < len(new_nodes_for_group):
                    new_assignments[new_nodes_for_group[i]] = collector
    else:
        df_all_candidate_nodes = df_collectors_time[df_collectors_time['collectors'].isna()]
        num_candidates_with_predicted_values = 5 * len(df_slow_collectors)
        df_nodes_with_predictions = df_all_candidate_nodes[(df_all_candidate_nodes['predicted_prob_long_waiting_time'] != -1)]
        df_nodes_without_predictions = df_all_candidate_nodes[(df_all_candidate_nodes['predicted_prob_long_waiting_time'] == -1)]
        candidates_with_predicted_values = df_nodes_with_predictions[df_nodes_with_predictions['predicted_prob_long_waiting_time'] < migration_prob].sample(
                                                                  n=num_candidates_with_predicted_values, random_state=seed)['machine'].tolist()
        num_candidates_without_predicted_values = int(len(df_nodes_without_predictions) / len(df_nodes_with_predictions) * num_candidates_with_predicted_values)
        candidates_without_predicted_values = df_nodes_without_predictions.sample(n=num_candidates_without_predicted_values)['machine'].tolist()
        print(f"num candidates with predicted values: {num_candidates_with_predicted_values}, num candidates without predicted values: {num_candidates_without_predicted_values}")
        new_nodes = random.sample(candidates_with_predicted_values + candidates_without_predicted_values, len(df_slow_collectors))
        for idx, collector in enumerate(df_slow_collectors['collectors'].tolist()):
            new_assignments[new_nodes[idx]] = collector
    migration_time = migrate_collectors(df_collectors_time, new_assignments, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, current_disk_date)
    return df_collectors_time, migration_time

def intelligent_schedule(df_collectors_machines, df_waiting_time,
                         df_cross_rack_migration, df_intra_rack_migration, df_forward_time,
                         start_time_point, end_time_point, start_disk_date,
                         migration_prob, timeout_threshold, timeout_migrating_threshold, seed):
    current_time_point = start_time_point
    current_disk_date = start_disk_date
    accumulated_waiting_time = 0
    while current_time_point <= end_time_point:
        print(f"\ncurrent_time_point={current_time_point}")
        df_collectors_time = merge_collectors_and_time(df_collectors_machines, df_waiting_time, current_time_point)
        df_predicted_slow_collectors = df_collectors_time[(~df_collectors_time['collectors'].isna()) & (df_collectors_time['predicted_prob_long_waiting_time'] >= 0.5)]
        print(
            f"predicted_slow_collectors=\n {df_predicted_slow_collectors[['collectors', 'rack', 'predicted_prob_long_waiting_time', 'actual_waiting_time']]} \n {len(df_predicted_slow_collectors)}")
        if len(df_predicted_slow_collectors) > 0:
            print("======Migrating based on prediction ====")
            print(f"predicted_slow_collectors=\n {df_predicted_slow_collectors[['collectors', 'machine', 'rack', 'predicted_prob_long_waiting_time', 'actual_waiting_time']]}")
            df_collectors_time, migration_time = intelligent_migration_hybrid(df_collectors_time, df_cross_rack_migration, df_intra_rack_migration,
                                                                              df_forward_time, df_predicted_slow_collectors,
                                                                              current_disk_date, migration_prob, seed)
            print(f"after migration =\n {df_collectors_time[~df_collectors_time['collectors'].isna()][['collectors', 'machine', 'rack', 'predicted_prob_long_waiting_time', 'actual_waiting_time']]}")
            # migration within rack
            accumulated_waiting_time += migration_time
        max_actual_waiting_time = df_collectors_time[~df_collectors_time['collectors'].isna()]['actual_waiting_time'].max()
        print(f"max_actual_waiting_time={max_actual_waiting_time}")
        timeout_migrating_counter = 0
        migrating_method = None
        while max_actual_waiting_time > timeout_threshold and timeout_migrating_counter < timeout_migrating_threshold:
            df_slow_collectors = df_collectors_time[(~df_collectors_time['collectors'].isna()) & (df_collectors_time['actual_waiting_time'] > timeout_threshold)]
            print("======Timeout migrating======")
            if timeout_migrating_counter < timeout_migrating_threshold - 1:
                migrating_method = "sorting"
            else:
                migrating_method = "random"
            df_collectors_time, migration_time = intelligent_migration_hybrid(df_collectors_time, df_cross_rack_migration, df_intra_rack_migration,
                                                                              df_forward_time, df_slow_collectors,
                                                                              current_disk_date, migration_prob, seed, migrating_method)
            print(f"after migration =\n {df_collectors_time[~df_collectors_time['collectors'].isna()][['collectors', 'rack', 'predicted_prob_long_waiting_time', 'actual_waiting_time']]}")
            accumulated_waiting_time += migration_time
            timeout_migrating_counter += 1
            max_actual_waiting_time = df_collectors_time[~df_collectors_time['collectors'].isna()]['actual_waiting_time'].max()
            print(f"max_actual_waiting_time={max_actual_waiting_time}")
            print(f"timeout migrating counter = {timeout_migrating_counter}")

        accumulated_waiting_time += (timeout_migrating_counter * timeout_threshold + max_actual_waiting_time)
        print(f"accumulated_waiting_time={accumulated_waiting_time}")
        # updated mapping between collectors and machines
        df_collectors_machines = df_collectors_time[['machine', 'gpu_type', 'cap_cpu', 'cap_mem', 'cap_gpu', 'rack', 'collectors']]
        current_time_point += pd.Timedelta('24H')
        current_disk_date += pd.Timedelta('24H')
    return accumulated_waiting_time

def main_for_intelligent_schedule(num_collectors, seed, migration_prob):
    df_machines, df_waiting_time, df_cross_rack_migration, df_intra_rack_migration, df_forward_time, rack_list, start_disk_date = read_data(num_collectors)
    accumulated_waiting_times = []
    timeout_threshold = 10
    timeout_migrating_threshold = 3
    for start_hour in range(24):
        random.seed(seed)
        start_time_point = pd.Timestamp(f"1970-02-17 {start_hour}:00:00+08:00")
        end_time_point = pd.Timestamp("1970-03-15 23:00:00+08:00")
        df_machines = assign_collectors_to_machines(rack_list, df_machines)
        print(f"df_machine = \n{df_machines[~df_machines['collectors'].isna()][['rack', 'machine', 'collectors']]}")
        accumulated_waiting_times.append(intelligent_schedule(df_machines, df_waiting_time,
                                                              df_cross_rack_migration, df_intra_rack_migration, df_forward_time,
                                                              start_time_point, end_time_point, start_disk_date, migration_prob,
                                                              timeout_threshold, timeout_migrating_threshold, seed))
    hours = [x for x in range(24)]
    res_df = pd.DataFrame({'hour': hours, 'accumulated_waiting_time': accumulated_waiting_times})
    res_df.to_csv(f'{SCHEDULER_RESULT_PATH}/intelligent_schedule_hybrid_random_migration_forward_c{num_collectors}_prob{migration_prob}_timeout{timeout_threshold}_migration{timeout_migrating_threshold}_candidate10_{seed}.csv', index=False)


if __name__ == '__main__':
    num_collectors = int(sys.argv[1])
    method = sys.argv[2]
    seed = int(sys.argv[3])
    if method == "intelligent":
        migration_prob = float(sys.argv[4])
        main_for_intelligent_schedule(num_collectors, seed, migration_prob)
    elif method == 'random':
        main_for_random_schedule(num_collectors, seed)
    elif method == 'random-timeout':
        main_for_random_timeout_schedule(num_collectors, seed)
    else:
        print(f"method {method} not implemented")
