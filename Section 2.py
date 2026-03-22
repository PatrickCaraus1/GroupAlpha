import pandas as pd
import numpy as np
import time
import os
import psutil

def start():
    t = time.perf_counter()
    return t

def stop(t):
    print(f"Time taken: {time.perf_counter()-t:.2f} seconds")

def get_memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 ** 2)
print(f"  Memory after load     : {get_memory_mb():.1f} MB")

print("Loading data...")
load = start()
df = pd.read_csv('data/IMDB TMDB Movie Metadata Big Dataset (1M).csv')
stop(load)
print(f"  Memory after load     : {get_memory_mb():.1f} MB")

print(f"\n{'=' * 70}")
print("DATASET OVERVIEW")
print(f"{'=' * 70}")
print(f"Total records: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"Columns: {list(df.columns)}")

print(f"\n{'=' * 70}")
print("KEY STATISTICS")
print(f"{'=' * 70}")

# Try to get budget and revenue - try different column names
budget_col = None
revenue_col = None

# Find columns
for col in df.columns:
    if 'budget' in col.lower() and budget_col is None:
        budget_col = col
    if 'revenue' in col.lower() and revenue_col is None:
        revenue_col = col

print(f"\nBudget column: {budget_col}")
print(f"Revenue column: {revenue_col}")

# If we found them, calculate ROI
if budget_col and revenue_col:
    print(f"\n{'=' * 70}")
    print("DATA CLEANING")
    print(f"{'=' * 70}")

    # Count nulls
    budget_nulls = df[budget_col].isna().sum()
    revenue_nulls = df[revenue_col].isna().sum()

    print(f"\nNull values:")
    print(f"  {budget_col}: {budget_nulls:,}")
    print(f"  {revenue_col}: {revenue_nulls:,}")

    # Clean data
    clean = start()
    df_clean = df[(df[budget_col] > 0) & (df[revenue_col] > 0)].copy()
    print(f"  Memory after cleaning : {get_memory_mb():.1f} MB")
    removed = len(df) - len(df_clean)
    removal_pct = (removed / len(df)) * 100

    print(f"\nOriginal records: {len(df):,}")
    print(f"After cleaning (budget > 0 AND revenue > 0): {len(df_clean):,}")
    print(f"Removed: {removed:,} ({removal_pct:.1f}%)")
    stop(clean)
    print(f"\n{'=' * 70}")
    print("ROI ANALYSIS")
    print(f"{'=' * 70}")

    # Calculate ROI
    Roi = start()
    df_clean['roi'] = (df_clean[revenue_col] - df_clean[budget_col]) / df_clean[budget_col]
    print(f"  Memory after ROI calc : {get_memory_mb():.1f} MB")
    print(f"\nROI Statistics:")
    print(f"  Average ROI: {df_clean['roi'].mean():.4f}")
    print(f"  Median ROI: {df_clean['roi'].median():.4f}")
    print(f"  Min ROI: {df_clean['roi'].min():.4f}")
    print(f"  Max ROI: {df_clean['roi'].max():.4f}")
    print(f"  Std Dev: {df_clean['roi'].std():.4f}")
    # Profitable vs unprofitable
    profitable = len(df_clean[df_clean['roi'] > 0])
    unprofitable = len(df_clean[df_clean['roi'] <= 0])

    print(f"\nProfitable movies (ROI > 0): {profitable:,} ({profitable / len(df_clean) * 100:.1f}%)")
    print(f"Loss-making movies (ROI <= 0): {unprofitable:,} ({unprofitable / len(df_clean) * 100:.1f}%)")
    stop(Roi)
print(f"\n{'=' * 70}")
print("KEY FINDINGS FOR YOUR REPORT")
print(f"{'=' * 70}")

if budget_col and revenue_col:
    print(f"""
 Dataset: {len(df):,} total movies
 After cleaning: {len(df_clean):,} valid records
 Data removed: {removed:,} records ({removal_pct:.1f}%)
 Average ROI: {df_clean['roi'].mean():.4f} ({df_clean['roi'].mean() * 100:.1f}% profit margin)
 ROI Range: {df_clean['roi'].min():.2f}x to {df_clean['roi'].max():.2f}x
 Profitable: {profitable:,} movies ({profitable / len(df_clean) * 100:.1f}%)

""")
else:
    print(f"""
Could not find budget or revenue columns.
Available columns: {list(df.columns)}
""")

print("Done!")



