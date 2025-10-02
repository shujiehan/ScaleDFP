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

import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from tqdm.auto import tqdm

# Import the Cython module
try:
    import resource_features_cython_module as rfc_cython
except ImportError:
    print("Warning: Cython module 'resource_features_cython_module' not found. "
          "Falling back to Python implementation for generate_resource_features_for_each_machine. "
          "Run 'pip install .' in the project root to compile the Cython module.")
    rfc_cython = None # Set to None to indicate fallback


resource_cols = ['waiting_time', 'cpu_usage', 'gpu_wrk_util', 'avg_mem',
                 'avg_gpu_wrk_mem', 'max_mem', 'max_gpu_wrk_mem']

def generate_resource_features(df):
    # Ensure submit_time and end_time are datetime objects and convert to nanoseconds
    df['submit_time'] = pd.to_datetime(df['submit_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    
    # Sort the entire DataFrame before grouping to ensure consistent order
    # for each machine when using .iloc, as multiprocessing shuffles group order.
    df = df.sort_values(by=['machine', 'submit_time']).reset_index(drop=True)


    grouped = df.groupby(['machine'])
    
    # Prepare groups for multiprocessing.
    # Convert DataFrames to dictionaries of NumPy arrays for C++/Cython
    machine_groups_for_fast_func = []
    for machine, group in grouped:
        group_dict = {
            'submit_time': group['submit_time'].astype(np.int64).values, # Convert to ns int64
            'end_time': group['end_time'].astype(np.int64).values,       # Convert to ns int64
        }
        for col in resource_cols:
            group_dict[col] = group[col].values.astype(np.float64) # Ensure float64
        
        # Store original index range for re-assembly later
        original_idx_start = group.index[0]
        original_idx_end = group.index[-1] + 1
        
        machine_groups_for_fast_func.append((machine, group_dict, (original_idx_start, original_idx_end)))

    num_processes = cpu_count() - 1 if cpu_count() > 1 else 1 
    if num_processes < 1: num_processes = 1 

    results_dfs = []
    with Pool(processes=num_processes) as pool:
        # We pass a tuple (machine_name, group_data_dict, original_idx_range) to the wrapper
        raw_results = list(tqdm(pool.imap(generate_resource_features_for_each_machine_wrapper, machine_groups_for_fast_func), 
                                total=len(machine_groups_for_fast_func), 
                                desc="Generating resource features for machines"))
    
    # Reconstruct DataFrames from results
    # Each item in raw_results will be a tuple: (original_group_df_index, result_features_array)
    for original_idx_range, features_array in raw_results:
        # Create a temporary DataFrame for the new features
        temp_df = pd.DataFrame(features_array)
        # Recreate column names
        new_feature_names = []
        for suffix in ['1h', '3h', '5h', '12h', '1d', '3d', '5d', '7d', '14d', '30d']:
            for col in resource_cols:
                for stat in ['avg', 'std', 'median', 'skew', 'kurt']:
                    new_feature_names.append(f'{stat}_prev_{col}_{suffix}')
        temp_df.columns = new_feature_names
        
        # Original DataFrame slice for this group
        original_group_df_slice = df.iloc[original_idx_range[0]:original_idx_range[1]].copy()
        
        # Merge the new features back into the original group's DataFrame slice
        # Ensure the index matches before concatenation
        temp_df.index = original_group_df_slice.index
        results_dfs.append(pd.concat([original_group_df_slice, temp_df], axis=1))

    res_df = pd.concat(results_dfs, axis=0)
    res_df = res_df.fillna(0) # Fill NaNs (for first rows where no historical data exists)
    return res_df


# Wrapper function for multiprocessing
def generate_resource_features_for_each_machine_wrapper(args):
    machine_name, group_data_dict, original_idx_range = args
    
    if rfc_cython:
        # Call the Cython function
        result_features_array = rfc_cython.calculate_resource_features_cython(
            group_data_dict, resource_cols
        )
    else:
        # Fallback to Python version if Cython module is not loaded
        # Reconstruct a DataFrame from the dict for the Python function
        temp_df = pd.DataFrame(group_data_dict)
        # Ensure original column types are restored for Python logic if necessary
        temp_df['submit_time'] = pd.to_datetime(temp_df['submit_time'])
        temp_df['end_time'] = pd.to_datetime(temp_df['end_time'])
        
        # Call the Python function, which modifies temp_df in place or returns new df
        processed_group_df = generate_resource_features_for_each_machine_python(temp_df)
        
        # Extract only the newly added feature columns for consistency with Cython output format
        existing_cols = list(group_data_dict.keys())
        new_feature_cols = [col for col in processed_group_df.columns if col not in existing_cols]
        result_features_array = processed_group_df[new_feature_cols].values

    return original_idx_range, result_features_array


# Original Python function for fallback
def generate_resource_features_for_each_machine_python(df):
    df = df.sort_values(by='submit_time').reset_index(drop=True)

    look_back_windows = {
        '1h': pd.Timedelta('1H'),
        '3h': pd.Timedelta('3H'),
        '5h': pd.Timedelta('5H'),
        '12h': pd.Timedelta('12H'),
        '1d': pd.Timedelta('24H'),
        '3d': pd.Timedelta('3D'),
        '5d': pd.Timedelta('5D'),
        '7d': pd.Timedelta('7D'),
        '14d': pd.Timedelta('14D'),
        '30d': pd.Timedelta('30D')
    }

    for col in resource_cols:
        for suffix in look_back_windows:
            df[f'avg_prev_{col}_{suffix}'] = np.nan

    for i in range(len(df)):
        current_row = df.iloc[i]
        current_submit_time = current_row['submit_time']

        historical_data_slice = df.iloc[:i]

        if historical_data_slice.empty:
            continue

        for suffix, window_td in look_back_windows.items():
            look_back_window_time = (current_submit_time - window_td).floor(freq='H')

            completed_historical_instances = historical_data_slice[
                (historical_data_slice['end_time'] <= current_submit_time) & 
                (historical_data_slice['end_time'] >= look_back_window_time)
            ]

            if not completed_historical_instances.empty:
                for col in resource_cols:
                    df.loc[i, f'avg_prev_{col}_{suffix}'] = completed_historical_instances[col].mean()
    return df
