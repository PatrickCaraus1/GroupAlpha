import pandas as pd
import numpy as np
import time
from resource import *

Timer = time.perf_counter()


df = pd.read_csv('data/IMDB TMDB Movie Metadata Big Dataset (1M).csv')



budget_col = None
revenue_col = None


for col in df.columns:
    if 'budget' in col.lower() and budget_col is None:
        budget_col = col
    if 'revenue' in col.lower() and revenue_col is None:
        revenue_col = col

print(f"\nBudget column: {budget_col}")
print(f"Revenue column: {revenue_col}")


if budget_col and revenue_col:


    budget_nulls = df[budget_col].isna().sum()
    revenue_nulls = df[revenue_col].isna().sum()

    # Clean data
    df_clean = df[(df[budget_col] > 0) & (df[revenue_col] > 0)].copy()

    removed = len(df) - len(df_clean)
    removal_pct = (removed / len(df)) * 100

    print(f"\nOriginal records: {len(df):,}")
    print(f"After cleaning (budget > 0 AND revenue > 0): {len(df_clean):,}")
    print(f"Removed: {removed:,} ({removal_pct:.1f}%)")

    # Calculate ROI
    df_clean['roi'] = (df_clean[revenue_col] - df_clean[budget_col]) / df_clean[budget_col]

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


print("FINAL REPORT")


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



Timer = time.perf_counter() - Timer

print(f"Time taken: {Timer:.2f} seconds")

print(getrusage(RUSAGE_SELF))