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

import os
import pandas as pd
import numpy as np
from config import *

from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler, TargetEncoder
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix, classification_report

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from imblearn.under_sampling import RandomUnderSampler
from utils.feature_generation import *
from utils.resource_features_cython import generate_resource_features


DATA_PATH = "./data"
df = pd.read_csv(f"{DATA_PATH}/instance_preprocessed.csv")
df['waiting_time'] = df['start_time'] - df['submit_time']
df['submit_time'] = df['submit_time'].apply(pd.Timestamp, unit='s', tz='Asia/Shanghai')
df['start_time'] = df['start_time'].apply(pd.Timestamp, unit='s', tz='Asia/Shanghai')
df['end_time'] = df['end_time'].apply(pd.Timestamp, unit='s', tz='Asia/Shanghai')
df = df.drop(['status', 'duration'], axis=1)

print(f"generating time based features")
df = generate_time_based_features(df)
print(f"generating metric features")
df = preprocess_metric_features(df)
print(f"generating statistical features")
df = generate_statistical_features(df)
print(f"generating resource features")
df = generate_resource_features(df)

base_features = temporal_features + metric_features + statistical_features + resource_features
num_features = len(base_features)
print(f"num_features = {num_features}")

df = df.sort_values(['submit_time']).reset_index(drop=True)

print(f"df shape is {df.shape}")

WAITING_TIME_THRESHOLD = 10 # seconds
SPLIT_THRESHOLD = 0.5
df['is_long_waiting_time'] = (df['waiting_time'] > WAITING_TIME_THRESHOLD).astype(int)
print(f"Target variable 'is_long_waiting_time' created based on threshold > {WAITING_TIME_THRESHOLD}s.")
print(df['is_long_waiting_time'].value_counts()) # Check class distribution
DOWN_SAMPLE=1
NUM_TREES=200

RESULT_PATH = './results'
os.makedirs(RESULT_PATH, exist_ok=True) # Ensure results directory exists

n_splits = 4
df_list = []
for i, (train_index, test_index) in enumerate(split(df, n_splits)):
    print(f"\nFold {i}:")
    print(f"  Train: index={train_index}")
    print(f"  Test:  index={test_index}")
    train_df = df.iloc[train_index].copy()
    test_df = df.iloc[test_index].copy()
    X_train = train_df[base_features]
    X_test = test_df[base_features]

    # Use the new binary target for classification
    y_train = train_df['is_long_waiting_time']
    y_test = test_df['is_long_waiting_time']

    #scaler = StandardScaler() # Feature scaling
    #X_train_scaled = scaler.fit_transform(X_train)
    #X_test_scaled = scaler.transform(X_test)
    X_train_scaled = X_train
    X_test_scaled = X_test

    # --- Downsampling the training data ---
    print(f"Original training set shape: X_train_scaled {X_train_scaled.shape}, y_train {y_train.shape}")
    print(f"Original y_train class distribution: {y_train.value_counts()}")

    rus = RandomUnderSampler(random_state=42, sampling_strategy=DOWN_SAMPLE)
    X_train_resampled, y_train_resampled = rus.fit_resample(X_train_scaled, y_train)

    print(f"Resampled training set shape: X_train_resampled {X_train_resampled.shape}, y_train_resampled {y_train_resampled.shape}")
    print(f"Resampled y_train class distribution: {y_train_resampled.value_counts()}")

    # Instantiate the RandomForestClassifier
    # We remove class_weight='balanced' here because we are explicitly balancing the dataset via downsampling
    model = RandomForestClassifier(n_estimators=NUM_TREES, random_state=42, n_jobs=-1, class_weight='balanced') 

    print("Training Random Forest Classifier...")
    #model.fit(X_train_scaled, y_train) # Fit with the binary target
    model.fit(X_train_resampled, y_train_resampled) # Fit with the binary target
    print("Random Forest Training Complete.")

    if hasattr(model, 'feature_importances_'):
        feature_importances = model.feature_importances_
        # Create a pandas Series for easy sorting and viewing
        importance_df = pd.Series(feature_importances, index=base_features)
        sorted_importance = importance_df.sort_values(ascending=False)

        print(f"\n--- Feature Importance for Fold {i} (Top 20) ---")
        print(sorted_importance.head(20))
        print(f"\n--- Feature Importance for Fold {i} (Bottom 20) ---")
        print(sorted_importance.tail(20))
    else:
        print(f"\n--- Feature importance not available for the chosen model in Fold {i}. ---")

    print("7. Evaluating Random Forest Classifier model...")
    predictions = (model.predict_proba(X_test_scaled)[:,1] >= SPLIT_THRESHOLD).astype(bool)
    probabilities = model.predict_proba(X_test_scaled)[:, 1] # Get probabilities for the positive class (1)

    test_df['predicted_is_long_waiting_time'] = predictions
    test_df['predicted_prob_long_waiting_time'] = probabilities
    
    accuracy = accuracy_score(y_test, predictions)
    precision = precision_score(y_test, predictions, zero_division=0)
    recall = recall_score(y_test, predictions, zero_division=0)
    f1 = f1_score(y_test, predictions, zero_division=0)
    
    try:
        roc_auc = roc_auc_score(y_test, probabilities)
    except ValueError:
        roc_auc = "N/A (single class in test set)" # Handle cases where only one class is present in y_test

    print(f"Random Forest Classification Performance (Fold {i}):")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1-Score: {f1:.4f}")
    print(f"ROC AUC: {roc_auc}")
    print("\nClassification Report:")
    print(classification_report(y_test, predictions, zero_division=0))
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, predictions))

    res_df = test_df[['submit_time', 'machine', 'waiting_time', 'is_long_waiting_time', 
        'predicted_is_long_waiting_time', 'predicted_prob_long_waiting_time']]
    df_list.append(res_df)

res_df = pd.concat(df_list, axis=0)
res_df.set_index('submit_time', inplace=True)
resampled_df = res_df.groupby(['machine'])[['waiting_time', 'predicted_prob_long_waiting_time']].resample('H').median().fillna(method='ffill').reset_index()
OUTPUT_CSV_PATH = f"{RESULT_PATH}/predictions_rf_classification_stat_features{num_features}_waiting{WAITING_TIME_THRESHOLD}_threshold{SPLIT_THRESHOLD}_down{DOWN_SAMPLE}_tree{NUM_TREES}_resample.csv"
resampled_df.to_csv(OUTPUT_CSV_PATH, index=False)
print(f"Predictions with classification saved to {OUTPUT_CSV_PATH}")
