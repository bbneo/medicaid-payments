import pandas as pd
import argparse
from tqdm import tqdm
import csv

def safe_ratio(numerator, denominator):
    """Avoid division by zero → return NaN"""
    return numerator / denominator if denominator != 0 else float('nan')


def detect_outliers(group_rows):
    """
    Input: list of dicts (rows for one HCPCS + month group)
    Returns: list of outlier row dicts (original rows + added OUTLIER_REASONS column)
    Flags if extreme in: total_paid, paid_per_benef, paid_per_claim, or unique_beneficiaries
    """
    if len(group_rows) < 4:
        return []

    # Convert to DataFrame
    df = pd.DataFrame(group_rows)

    # Force numeric columns (handle junk strings, empty values, etc.)
    numeric_cols = ['TOTAL_PAID', 'TOTAL_UNIQUE_BENEFICIARIES', 'TOTAL_CLAIMS']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop completely invalid rows for stats
    df = df.dropna(subset=numeric_cols)

    if len(df) < 4:
        return []

    # Compute derived metrics (only for comparison)
    df['PAID_PER_BENEF'] = df.apply(
        lambda r: safe_ratio(r['TOTAL_PAID'], r['TOTAL_UNIQUE_BENEFICIARIES']), axis=1
    )
    df['PAID_PER_CLAIM'] = df.apply(
        lambda r: safe_ratio(r['TOTAL_PAID'], r['TOTAL_CLAIMS']), axis=1
    )

    # Drop rows invalid for ratio-based metrics
    df_clean = df.dropna(subset=['PAID_PER_BENEF', 'PAID_PER_CLAIM'])

    if len(df_clean) < 4:
        return []

    # ────────────────────────────────────────────────
    # IQR upper bound function (only upper outliers)
    # ────────────────────────────────────────────────
    def get_upper_bound(series):
        if len(series) < 4:
            return float('inf')  # skip tiny groups
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        return q3 + 1.5 * iqr

    # Compute thresholds for all four metrics
    bounds = {
        'high_total_paid': get_upper_bound(df_clean['TOTAL_PAID']),
        'high_paid_per_benef': get_upper_bound(df_clean['PAID_PER_BENEF']),
        'high_paid_per_claim': get_upper_bound(df_clean['PAID_PER_CLAIM']),
        'high_unique_benef': get_upper_bound(df_clean['TOTAL_UNIQUE_BENEFICIARIES']),
    }

    # Identify outliers using the cleaned df indices, but return original dicts
    flagged_rows = []
    for idx in df.index:
        row = df.loc[idx]
        flags = []

        # Raw total paid
        if pd.notna(row['TOTAL_PAID']) and row['TOTAL_PAID'] > bounds['high_total_paid']:
            flags.append('high_total_paid')

        # Paid per beneficiary
        if pd.notna(row['PAID_PER_BENEF']) and row['PAID_PER_BENEF'] > bounds['high_paid_per_benef']:
            flags.append('high_paid_per_benef')

        # Paid per claim
        if pd.notna(row['PAID_PER_CLAIM']) and row['PAID_PER_CLAIM'] > bounds['high_paid_per_claim']:
            flags.append('high_paid_per_claim')

        # High number of unique beneficiaries
        if pd.notna(row['TOTAL_UNIQUE_BENEFICIARIES']) and row['TOTAL_UNIQUE_BENEFICIARIES'] > bounds['high_unique_benef']:
            flags.append('high_unique_beneficiaries')

        if flags:
            original_row = group_rows[idx]  # preserve original string formatting etc.
            original_row['OUTLIER_REASONS'] = ','.join(flags)
            flagged_rows.append(original_row)

    return flagged_rows


def main(csv_file, output_file='outlier_billing_providers_multi.csv'):
    print(f"Streaming {csv_file} (expects sorted by HCPCS_CODE, CLAIM_FROM_MONTH)...")
    print("Flagging upper outliers on: total_paid, paid_per_beneficiary, paid_per_claim, unique_beneficiaries")

    outlier_rows = []
    current_group_rows = []
    current_key = None
    total_outliers = 0
    total_rows_processed = 0

    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # Add our new column to output
        fieldnames = reader.fieldnames + ['OUTLIER_REASONS']

        for row in tqdm(reader, desc="Streaming rows", unit="row"):
            total_rows_processed += 1
            key = (row['HCPCS_CODE'], row['CLAIM_FROM_MONTH'])

            if key != current_key and current_group_rows:
                outliers = detect_outliers(current_group_rows)
                if outliers:
                    outlier_rows.extend(outliers)
                    total_outliers += len(outliers)
                current_group_rows = []

            current_key = key
            current_group_rows.append(row)

        # Process final group
        if current_group_rows:
            outliers = detect_outliers(current_group_rows)
            if outliers:
                outlier_rows.extend(outliers)
                total_outliers += len(outliers)

    print(f"\nFinished. Processed {total_rows_processed:,} rows.")

    if total_outliers == 0:
        print("No outliers detected in any metric.")
        return

    # Write results
    with open(output_file, 'w', newline='', encoding='utf-8') as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(outlier_rows)

    print(f"Found and saved {total_outliers:,} outlier rows → {output_file}")
    print("Each outlier row includes 'OUTLIER_REASONS' column listing which metric(s) triggered the flag.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Streaming outlier detection on Medicaid claims (sorted CSV). "
                    "Flags upper outliers in four metrics relative to same HCPCS + month peers."
    )
    parser.add_argument('csv_file', help="Path to input CSV sorted by HCPCS_CODE, then CLAIM_FROM_MONTH")
    parser.add_argument('--output', default='outlier_billing_providers_multi.csv',
                        help="Output CSV path (default: outlier_billing_providers_multi.csv)")
    args = parser.parse_args()

    main(args.csv_file, args.output)

