import pandas as pd
import numpy as np


def add_time_features(df):
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    df['transaction_hour'] = df['transaction_time'].dt.hour
    df['transaction_day'] = df['transaction_time'].dt.day
    df['transaction_month'] = df['transaction_time'].dt.month
    df['transaction_dayofweek'] = df['transaction_time'].dt.dayofweek
    df['is_weekend'] = (df['transaction_dayofweek'] >= 5).astype(int)
    df['is_night'] = ((df['transaction_hour'] >= 22) | (df['transaction_hour'] <= 6)).astype(int)
    df = df.drop('transaction_time', axis=1)
    return df


def add_distance_features(df):
    if all(col in df.columns for col in ['lat', 'lon', 'merchant_lat', 'merchant_lon']):
        df['distance_simple'] = np.sqrt((df['lat'] - df['merchant_lat']) ** 2 + (df['lon'] - df['merchant_lon']) ** 2)
    return df


def process_categorical_features(df):
    categorical_cols = ['merch', 'cat_id', 'name_1', 'name_2', 'gender', 'street', 'one_city', 'us_state', 'post_code',
                        'jobs']
    categorical_cols = [col for col in categorical_cols if col in df.columns]
    for col in categorical_cols:
        df[col] = df[col].astype(str)
        if df[col].nunique() > 10:
            top_categories = df[col].value_counts().head(10).index
            df[col] = df[col].apply(lambda x: x if x in top_categories else 'other')
    return df


def process_numerical_features(df):
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            df[col] = df[col].fillna(df[col].median())
    large_value_cols = ['amount', 'population_city'] if 'amount' in df.columns else []
    for col in large_value_cols:
        if col in df.columns:
            df[f'{col}_log'] = np.log1p(df[col])
    return df


def add_feature_interactions(df):
    if 'amount' in df.columns and 'distance_simple' in df.columns:
        df['amount_distance_interaction'] = df['amount'] * df['distance_simple']
    if 'transaction_hour' in df.columns and 'is_weekend' in df.columns:
        df['hour_weekend_interaction'] = df['transaction_hour'] * df['is_weekend']
    return df


def load_train_data():
    train_df = pd.read_csv('./train_data/train.csv')
    train_processed = run_preprocessing(train_df)
    return train_processed


def run_preprocessing(input_df):
    df = input_df.copy()
    df = add_time_features(df)
    df = add_distance_features(df)
    df = process_categorical_features(df)
    df = process_numerical_features(df)
    df = add_feature_interactions(df)
    return df
