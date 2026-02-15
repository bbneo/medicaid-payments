

require(readr)
require(dplyr)
require(magrittr)
library(ggplot2)
library(lubridate)   # for easy ym() or ymd() conversion

medicaid_outliersfn = "medicaid-provider-outliers.csv"
medicaid_outliersdf = read.csv(medicaid_outliersfn)

nppesfn = "npidata_pfile_20050523-20260208.csv"
nppesfn = "npidata_test.csv"
nppespath = paste0("./nppes/", nppesfn)
nppesdf = read.csv(nppespath)

hcpcsfn = "HCPC2026_JAN_ANWEB_01122026.xlsx"
hcpcspath = paste0("./hcpcs/",hcpcsfn)
hcpcsdf = readxl::read_xlsx(hcpcspath)
hcpcs_shortdf = hcpcsdf %>% select( HCPCS_CODE = HCPC, SHORT_DESC = `SHORT DESCRIPTION`)


medicaid_outliersdf %>% colnames()

medicaid_outliersdf %>% group_by(HCPCS_CODE) %>% summarize(TOTAL_CLAIMS)


library(dplyr)  # Assuming magrittr is loaded via dplyr or separately if needed

# Example 1: Average total claims by HCPCS code
medicaid_outliersdf %>%
  group_by(HCPCS_CODE) %>%
  summarise(avg_total_claims = mean(TOTAL_CLAIMS, na.rm = TRUE)) %>%
  arrange(desc(avg_total_claims))

# Example 2: Average total paid by HCPCS code
medicaid_outliersdf %>%
  group_by(HCPCS_CODE) %>%
  summarise(avg_total_paid = mean(TOTAL_PAID, na.rm = TRUE)) %>% 
  arrange(desc(avg_total_paid))

# Example 3: Total unique beneficiaries by HCPCS code
medicaid_outliersdf %>%
  group_by(HCPCS_CODE) %>%
  summarise(total_unique_beneficiaries = sum(TOTAL_UNIQUE_BENEFICIARIES, na.rm = TRUE))

# Example 4: Average total claims by claim month
medicaid_outliersdf %>%
  group_by(CLAIM_FROM_MONTH) %>%
  summarise(avg_total_paid = mean(TOTAL_PAID, na.rm = TRUE)) %>%
  print(n=84)

# Example 5: Summary by HCPCS and outlier reasons (e.g., count of occurrences)
medicaid_outliersdf %>%
  group_by(HCPCS_CODE, OUTLIER_REASONS) %>%
  summarise(count = n(),
            avg_total_claims = mean(TOTAL_CLAIMS, na.rm = TRUE)) %>%
  filter(count>100) %>% print(n=100)

# Example 6: Overall summaries (no grouping)
medicaid_outliersdf %>%
  summarise(total_claims_overall = sum(TOTAL_CLAIMS, na.rm = TRUE),
            avg_paid_overall = mean(TOTAL_PAID, na.rm = TRUE),
            unique_hcpcs_count = n_distinct(HCPCS_CODE))

require(scales)   # ← key package for nice number formatting

medicaid_outliersdf %>%
  group_by(OUTLIER_REASONS) %>%
  summarise(
    count                  = n(),
    avg_total_unique_benef = mean(TOTAL_UNIQUE_BENEFICIARIES, na.rm = TRUE),
    avg_total_claims       = mean(TOTAL_CLAIMS, na.rm = TRUE)
  ) %>%
  mutate(
    count                  = comma(count, accuracy = 1),                     # 1,250,000
    avg_total_unique_benef = number(avg_total_unique_benef, accuracy = 0.1), # 19.6
    avg_total_claims       = number(avg_total_claims,       accuracy = 0.1)  # 97.9
  ) %>%
  print(width = Inf)   # optional: prevents line wrapping

medicaid_outliersdf %>%
  filter(OUTLIER_REASONS == "high_total_paid,high_paid_per_benef,high_unique_beneficiaries") %>%
  arrange(desc(TOTAL_PAID)) # %>% print(n=50)


