import pandas as pd
import argparse
from tqdm import tqdm
from pathlib import Path

def detect_outliers(group):
    if len(group) < 4:
        return pd.DataFrame()
    Q1 = group['TOTAL_PAID'].quantile(0.25)
    Q3 = group['TOTAL_PAID'].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + 1.5 * IQR
    return group[group['TOTAL_PAID'] > upper_bound]

def main(csv_file, chunk_size=250_000, output_file='outlier_billing_providers.csv'):
    csv_path = Path(csv_file)
    print(f"Processing {csv_path.name} in chunks of {chunk_size:,} rows...")

    # We'll collect outlier DataFrames and write incrementally
    first_write = True
    total_outliers = 0
    total_rows = 0

    # We need to group across the whole file → accumulate partial groups
    from collections import defaultdict
    partial_groups = defaultdict(list)

    chunks = pd.read_csv(
        csv_file,
        parse_dates=['CLAIM_FROM_MONTH'],
        dtype={
            'BILLING_PROVIDER_NPI_NUM': 'str',
            'SERVICING_PROVIDER_NPI_NUM': 'str',
            'HCPCS_CODE': 'str',
            'TOTAL_UNIQUE_BENEFICIARIES': 'int',
            'TOTAL_CLAIMS': 'int',
            'TOTAL_PAID': 'float'
        },
        chunksize=chunk_size
    )

    for chunk in tqdm(chunks, desc="Reading & processing chunks", unit="chunk"):
        total_rows += len(chunk)

        # Group within this chunk
        for (hcpcs, month), sub_df in chunk.groupby(['HCPCS_CODE', 'CLAIM_FROM_MONTH']):
            partial_groups[(hcpcs, month)].append(sub_df)

        # Process any groups that are "complete enough" (heuristic: if we saw this group before)
        # But to keep it simple & correct: we defer full processing until end

    print(f"Finished reading {total_rows:,} rows. Now finalizing groups...")

    outlier_dfs = []
    group_keys = list(partial_groups.keys())
    for key in tqdm(group_keys, desc="Detecting outliers", unit="group"):
        full_group = pd.concat(partial_groups[key], ignore_index=True)
        outliers = detect_outliers(full_group)
        if not outliers.empty:
            outlier_dfs.append(outliers)
        del partial_groups[key]  # free memory

    if not outlier_dfs:
        print("No outliers detected.")
        return

    outliers_df = pd.concat(outlier_dfs, ignore_index=True)
    total_outliers = len(outliers_df)

    outliers_df.to_csv(output_file, index=False)
    print(f"\nFound {total_outliers:,} outlier rows → saved to {output_file}")

    summary = outliers_df.groupby(['HCPCS_CODE', 'CLAIM_FROM_MONTH']).size().reset_index(name='Outlier Count')
    print("\nSummary of outliers by HCPCS and month:")
    print(summary)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Memory-efficient outlier detection on large Medicaid claims CSV")
    parser.add_argument('csv_file', type=str, help="Path to the input CSV file")
    parser.add_argument('--chunk_size', type=int, default=250000, help="Rows per chunk (default: 250000)")
    args = parser.parse_args()
    main(args.csv_file, args.chunk_size)

