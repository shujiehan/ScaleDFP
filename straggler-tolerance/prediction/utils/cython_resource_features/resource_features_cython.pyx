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

# cython_resource_features/resource_features_cython.pyx
from libcpp.vector cimport vector
from libcpp.algorithm cimport sort
from libc.math cimport sqrt, pow

import numpy as np
cimport numpy as np
import pandas as pd # Used for pd.Timedelta in python wrapper, but not directly here

# Declare functions/variables that are external or will be used with C types
# Using cpdef allows the function to be called from both Python and C
cpdef np.ndarray[np.float64_t, ndim=2] calculate_resource_features_cython(
    dict df_dict,
    list resource_cols_py
):
    cdef int num_rows
    cdef np.ndarray[np.int64_t, ndim=1] submit_times_ns = df_dict['submit_time']
    cdef np.ndarray[np.int64_t, ndim=1] end_times_ns = df_dict['end_time']

    num_rows = submit_times_ns.shape[0]
    if num_rows == 0:
        return np.empty((0,0), dtype=np.float64) # Return empty array for empty input

    cdef list resource_cols_names = []
    cdef np.ndarray[np.float64_t, ndim=1, mode='c'] col_data_arr # Temporary variable for casting
    # Use a C++ vector to store raw pointers to the resource column data
    cdef vector[double*] resource_col_data_ptrs
    
    # Acquire GIL to interact with Python list `resource_cols_py` and dict `df_dict`
    # and to get NumPy array data pointers.
    for col_name in resource_cols_py:
        resource_cols_names.append(col_name) # This list remains a Python list
        col_data_arr = df_dict[col_name] # Get the NumPy array object
        resource_col_data_ptrs.push_back(<double*>np.PyArray_DATA(col_data_arr)) # Get raw C pointer and store it

    cdef int num_resource_cols = len(resource_cols_names)

    cdef int num_features_per_row = num_resource_cols * 10 * 5 # avg, std, median, skew, kurtosis * 5 window values
  
    cdef np.ndarray[np.float64_t, ndim=2, mode='c'] result_features_array = np.zeros(
        (num_rows, num_features_per_row), dtype=np.float64
    )
    
    # Get raw pointers for C-level access for fixed arrays
    cdef long long* submit_times_data = <long long*>np.PyArray_DATA(submit_times_ns)
    cdef long long* end_times_data = <long long*>np.PyArray_DATA(end_times_ns)
    #cdef double* result_data = <double*>np.PyArray_DATA(result_features_array)

    cdef long long window_vals[10] # For '1h', '3h', '5h', '12h', '1d', '3d', '5d', '7d', '14d', '30d'
    window_vals[0] = 3600_000_000_000
    window_vals[1] = 3 * 3600_000_000_000
    window_vals[2] = 5 * 3600_000_000_000
    window_vals[3] = 12 * 3600_000_000_000
    window_vals[4] = 24 * 3600_000_000_000
    window_vals[5] = 3* 24 * 3600_000_000_000
    window_vals[6] = 5 * 24 * 3600_000_000_000
    window_vals[7] = 7 * 24 * 3600_000_000_000
    window_vals[8] = 14 * 24 * 3600_000_000_000
    window_vals[9] = 30 * 24 * 3600_000_000_000

    cdef vector[double] window_data_points # To collect values for std, median, skew, kurtosis

    cdef int i, j, col_idx, window_idx # Declare loop variables as C integers
    cdef int feature_idx_offset
    cdef double* current_res_col_ptr # C pointer for current resource column data

    cdef long long current_submit_time_val
    cdef long long historical_end_time_val
    cdef long long look_back_window_time_val
    cdef double sum_val
    cdef int count
    cdef double avg_val
    cdef double std_val
    cdef double median_val
    cdef double skew_val
    cdef double kurt_val

    cdef double sum_sq_diff
    cdef double m2 # second central moment (variance)
    cdef double m3 # third central moment
    cdef double m4 # fourth central moment
    cdef double diff # Declare diff here

    # Main computation loop. Release GIL here as all data access is now via C pointers.
    with nogil:
        # Use C-style for loop for `range(num_rows)`
        for i from 0 <= i < num_rows:
            current_submit_time_val = submit_times_data[i] # Access via C pointer
            feature_idx_offset = 0

            for window_idx from 0 <= window_idx < 5: # Iterate based on index for fixed order
                look_back_window_time_val = current_submit_time_val - window_vals[window_idx]

                for col_idx from 0 <= col_idx < num_resource_cols: # Iterate using C-style loop
                    current_res_col_ptr = resource_col_data_ptrs[col_idx] # Access C pointer from C++ vector
                    
                    window_data_points.clear() # Clear for each new window/column combination

                    # Inner loop for historical data (iloc[:i])
                    for j from 0 <= j < i: # Iterate using C-style loop
                        historical_end_time_val = end_times_data[j] # Access via C pointer
                        
                        if historical_end_time_val <= current_submit_time_val and \
                           historical_end_time_val >= look_back_window_time_val:
                            window_data_points.push_back(current_res_col_ptr[j])
                    
                    count = window_data_points.size()

                    avg_val = 0.0
                    std_val = 0.0
                    median_val = 0.0
                    skew_val = 0.0
                    kurt_val = 0.0
    
                    if count > 0:
                        sum_val = 0.0
                        for val in window_data_points:
                            sum_val += val
                        avg_val = sum_val / count

                        # Initialize here for each new column/window combination
                        sum_sq_diff = 0.0
                        m2 = 0.0
                        m3 = 0.0
                        m4 = 0.0
                    
                        # Calculate Standard Deviation
                        if count > 1: # Standard deviation requires at least 2 points for sample std
                            for val in window_data_points:
                                sum_sq_diff += pow(val - avg_val, 2)
                            std_val = sqrt(sum_sq_diff / (count - 1)) # Sample standard deviation
                        else:
                            std_val = 0.0 # If only one point, std dev is 0.

                        # Calculate Median
                        sort(window_data_points.begin(), window_data_points.end())
                        if count % 2 == 1:
                            median_val = window_data_points[count // 2]
                        else:
                            median_val = (window_data_points[count // 2 - 1] + window_data_points[count // 2]) / 2.0

                        # Calculate Skewness and Kurtosis (Fisher's definition for kurtosis)
                        if count > 2:
                            for val in window_data_points:
                                diff = val - avg_val # Assign value to already declared 'diff'
                                m2 += pow(diff, 2)
                                m3 += pow(diff, 3)
                                m4 += pow(diff, 4)

                            m2 /= count
                            m3 /= count
                            m4 /= count

                            # Skewness
                            if m2 > 0: # Avoid division by zero if variance is zero
                                skew_val = m3 / pow(m2, 1.5)
                            else:
                                skew_val = 0.0

                            # Kurtosis (excess kurtosis)
                            if m2 > 0:
                                kurt_val = m4 / pow(m2, 2.0) - 3.0 # Fisher's kurtosis
                            else:
                                kurt_val = 0.0
                    
                    # Store result in the C-level output array
                    result_features_array[i, feature_idx_offset] = avg_val
                    feature_idx_offset += 1
                    result_features_array[i, feature_idx_offset] = std_val
                    feature_idx_offset += 1
                    result_features_array[i, feature_idx_offset] = median_val
                    feature_idx_offset += 1
                    result_features_array[i, feature_idx_offset] = skew_val
                    feature_idx_offset += 1
                    result_features_array[i, feature_idx_offset] = kurt_val
                    feature_idx_offset += 1
    
    return result_features_array
