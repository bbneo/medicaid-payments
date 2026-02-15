The Python script performs **peer-grouped, procedure-specific outlier detection** on Medicaid claims data to identify **potentially anomalous billing providers** (identified by `BILLING_PROVIDER_NPI_NUM`).

### Core Approach & What It Accomplishes

1. **Grouping / Peer Comparison Level**  
   - Data is processed grouped by **HCPCS_CODE** (procedure/service code) + **CLAIM_FROM_MONTH** (month of service).  
   - This creates narrow **peer groups**: all billing providers who submitted claims for the exact same procedure in the exact same month.  
   - Outliers are detected **relative to these peers** (not globally across all providers/codes/times), which is the standard method in healthcare fraud/waste/abuse (FWA) analytics for fair, apples-to-apples comparisons.

2. **Metrics Flagged as Outliers** (upper tail only, using IQR method)  
   The script flags a provider as an outlier if they exceed the **1.5 × IQR upper bound** in **any** of these four metrics within their HCPCS + month peer group:

   | Metric                        | What it measures                              | Typical fraud/abuse signal being caught                  | Output reason tag              |
   |-------------------------------|-----------------------------------------------|----------------------------------------------------------|--------------------------------|
   | **TOTAL_PAID**                | Raw total Medicaid dollars paid               | Extremely high absolute reimbursement amounts            | `high_total_paid`              |
   | **TOTAL_PAID per beneficiary** | Average payment per unique patient            | Excessive $/patient (possible upcoding, unbundling)      | `high_paid_per_benef`          |
   | **TOTAL_PAID per claim**      | Average reimbursement per service line        | Inflated payment per individual claim                    | `high_paid_per_claim`          |
   | **TOTAL_UNIQUE_BENEFICIARIES**| Number of unique patients attributed          | Implausibly large patient panel (phantom patients, etc.) | `high_unique_beneficiaries`    |

3. **Outlier Definition & Output**  
   - Uses the classic **IQR-based upper outlier rule** (Q3 + 1.5 × IQR).  
   - A single provider can be flagged for **one or more** reasons in the same group.  
   - Results are written to CSV with an added column **`OUTLIER_REASONS`** (comma-separated list of which metric(s) triggered the flag).  
   - Only **upper** outliers are considered (no lower-tail flags, as under-billing is rarely a fraud concern).

4. **Key Characteristics of the Method**  
   - **Procedure-specific & time-bound** → detects anomalies tied to specific services/months (e.g., a provider massively over-billing one HCPCS code in January).  
   - **Multi-dimensional** → catches different flavors of suspicious billing (high volume, high intensity, high price per unit, inflated patient counts).  
   - **Streaming / memory-efficient** → designed for large datasets (requires pre-sorted input CSV).  
   - **Conservative small-group handling** → skips IQR if <4 providers in a peer group (avoids unreliable statistics).

In summary:  
This code implements a **classic Medicaid/Medicare program integrity-style outlier screening pipeline**, focused on identifying **billing providers whose behavior is statistically extreme compared to their peers for the same procedure in the same month**, across four complementary financial and volume metrics commonly used in fraud detection workflows.
