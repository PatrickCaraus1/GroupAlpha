from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pyspark import SparkContext, SparkConf
import time
import os, sys
# os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-21.0.8.9-hotspot"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


conf = SparkConf().setAppName("MovieROIAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


DB_URL = "sqlite:///IMDB.db"
engine = create_engine(DB_URL, echo=False)
Session = sessionmaker(bind=engine)

Timer = time.perf_counter()

def inspect_db() -> tuple[str, str | None, str | None]:

    with engine.connect() as conn:
        # List all tables
        tables = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table'")
        ).fetchall()
        table_names = [t[0] for t in tables]
        print(f"Tables found in IMDB.db: {table_names}")

        if not table_names:
            raise RuntimeError("No tables found in IMDB.db")

        # Use the first table (adjust if needed)
        table_name = table_names[0]
        print(f"Using table: '{table_name}'")

        # Discover columns via PRAGMA
        columns = conn.execute(
            text(f"PRAGMA table_info({table_name})")
        ).fetchall()
        col_names = [c[1] for c in columns]
        print(f"Columns: {col_names}")

        budget_col  = next((c for c in col_names if "budget"  in c.lower()), None)
        revenue_col = next((c for c in col_names if "revenue" in c.lower()), None)

        return table_name, budget_col, revenue_col


def fetch_records(table_name: str, budget_col: str, revenue_col: str) -> list[dict]:

    with Session() as session:
        result = session.execute(
            text(f'SELECT "{budget_col}", "{revenue_col}" FROM {table_name}')
        )
        return [{"budget": r[0], "revenue": r[1]} for r in result]


def analyse_with_rdd(records: list[dict]) -> None:




    raw_rdd = sc.parallelize(records)
    total_records = raw_rdd.count()
    print(f"Total records loaded from DB: {total_records:,}")




    clean_rdd = raw_rdd.filter(
        lambda r: r["budget"]  is not None and r["budget"]  > 0
               and r["revenue"] is not None and r["revenue"] > 0
    )
    clean_count = clean_rdd.count()
    removed     = total_records - clean_count
    # print(f"\nOriginal records : {total_records:,}")
    # print(f"After cleaning   : {clean_count:,}")
    # print(f"Removed          : {removed:,} ({removed / total_records * 100:.1f}%)")


    roi_rdd = clean_rdd.map(
        lambda r: (r["revenue"] - r["budget"]) / r["budget"]
    ).cache()

    stats = roi_rdd.map(
        lambda x: (x, x, x, x * x, 1)
    ).reduce(
        lambda a, b: (
            min(a[0], b[0]),
            max(a[1], b[1]),
            a[2] + b[2],
            a[3] + b[3],
            a[4] + b[4],
        )
    )

    roi_min, roi_max, roi_sum, roi_sum_sq, n = stats
    roi_mean = roi_sum / n
    roi_std  = ((roi_sum_sq / n) - (roi_mean ** 2)) ** 0.5


    sorted_sample = sorted(roi_rdd.takeSample(False, min(n, 100_000), seed=42))
    roi_median    = sorted_sample[len(sorted_sample) // 2]

    profitable   = roi_rdd.filter(lambda x: x > 0).count()
    unprofitable = n - profitable


    print(f"\n{'='*70}")
    print("ROI ANALYSIS")
    print(f"{'='*70}")
    print(f"\nROI Statistics:")
    print(f"  Average ROI : {roi_mean:.4f}")
    print(f"  Median ROI  : {roi_median:.4f}  (sampled)")
    print(f"  Min ROI     : {roi_min:.4f}")
    print(f"  Max ROI     : {roi_max:.4f}")
    print(f"  Std Dev     : {roi_std:.4f}")
    print(f"\nProfitable movies  (ROI > 0) : {profitable:,}   ({profitable / n * 100:.1f}%)")
    print(f"Loss-making movies (ROI ≤ 0) : {unprofitable:,}   ({unprofitable / n * 100:.1f}%)")

    print(f"\n{'='*70}")
    print("KEY FINDINGS")
    print(f"{'='*70}")
    print(f"""
  Dataset       : {total_records:,} total movies
  After cleaning: {clean_count:,} valid records
  Data removed  : {removed:,} records ({removed / total_records * 100:.1f}%)
  Average ROI   : {roi_mean:.4f}  ({roi_mean * 100:.1f}% profit margin)
  ROI range     : {roi_min:.2f}x  →  {roi_max:.2f}x
  Profitable    : {profitable:,} movies ({profitable / n * 100:.1f}%)
""")


def main():
    table_name, budget_col, revenue_col = inspect_db()

    if not budget_col or not revenue_col:
        print(f"ERROR: Could not locate budget/revenue columns.")
        print(f"  budget  → {budget_col}")
        print(f"  revenue → {revenue_col}")
        sc.stop()
        return

    print(f"\nBudget column  : {budget_col}")
    print(f"Revenue column : {revenue_col}")

    records = fetch_records(table_name, budget_col, revenue_col)
    analyse_with_rdd(records)

    sc.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()

Timer = time.perf_counter() - Timer

print(f"Time taken: {Timer:.2f} seconds")
