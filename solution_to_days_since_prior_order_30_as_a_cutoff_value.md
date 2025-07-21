# Solution to `days_since_prior_order == 30` as a Cutoff Value

This document explains why the `days_since_prior_order` field in the Instacart dataset cannot be treated as a simple 30‑day maximum, and describes our refined approach to handle this censoring.

---

## 1. Problem Overview

- **Missing True Timestamps**: Orders only record
  
  - the day of week (`order_dow`),
  - the hour of day (`order_hour_of_day`),
  - and an inter‑order interval capped at 30 days (`days_since_prior_order`).

- **Censoring Effect**: Any interval of 30 days or more is recorded as exactly 30.
  - Directly using `order_date = today() – days_since_prior_order` causes a massive spike at the “30‑day” cutoff in visualizations.
  - Features derived from this field (e.g. `recency_days`) severely underestimate longer intervals.

---

## 2. Why Traditional Fixes Fall Short

1. **Relative‑Time Metrics** (cumulated days, frequency, recency) avoid calendar artifacts but lose alignment with business events (promotions, holidays).
2. **Pseudo‑Calendar Reconstruction + Jitter** spreads out the 30‑day spike, yet:
   - Jitter distorts real calendar alignment and may obscure weekly patterns.
   - It forces a trade‑off between flattening the cutoff peak and preserving true periodicity.

Neither approach fully addresses the statistical bias introduced by censoring.

---

## 3. Our Refined Solution: Multiple Imputation under MNAR

### 3.1 Identify Missingness Mechanism

- **First‑order NA** (no prior order) are MCAR and set to 0 days.
- **Censoring at 30** is MNAR: the probability of being recorded as 30 depends on the unseen true interval.  
  - Statistical tests show a significant correlation between truncation frequency and user behavior (frequency, recency).

### 3.2 Apply Multiple Imputation (MI)

1. **Treat all `==30` entries as missing** for imputation.
2. **Use Predictive Mean Matching** to generate multiple (m) plausible values in place of 30.
3. **Produce m complete datasets**, each reflecting different scenarios of long‑interval occurrence.
4. **Recompute key features** (e.g. `recency_days`, `frequency`) in each dataset, then **pool** results to capture uncertainty.

This MI framework corrects bias, retains plausible long intervals, and quantifies imputation uncertainty.

---

## 4. Monitoring and Quality Control

- **Data Quality Metrics**: Track and alert on daily/weekly proportions of:
  - Missing (`NA`) `days_since_prior_order` (first orders).
  - Censored (`==30`) intervals.
- **Automated Dashboards**: Use Athena or QuickSight to visualize trends; set thresholds (e.g. >15%) for automatic notifications.

---

## 5. Sensitivity Analysis

To ensure model robustness against censoring assumptions, we perform:

1. **Best‑ vs Worst‑Case Scenarios**
   - Best: treat all censored values as the original 30 days.
   - Worst: treat all as a longer interval (e.g. 60 days).
   - Compare resulting distributions of `recency_days` to bound the effect of extreme assumptions.

2. **Delta‑Adjustment**
   - Systematically vary the censored value by ±Δ (e.g. –5, 0, +5, +15 days).
   - Observe linear shifts in key statistics (mean, median, IQR) to confirm predictable sensitivity.

These analyses demonstrate how feature values and downstream model predictions change under different censoring hypotheses.

---

## 6. Recommendations and Next Steps

1. **Adopt MI for all censoring** in the production pipeline, with m and model parameters tuned for efficiency and convergence.
2. **Implement continuous monitoring** of missing/censored proportions in ETL workflows.
3. **Incorporate sensitivity results** into model evaluation and business documentation, providing confidence intervals instead of point estimates.
4. **Explore stratified imputation** or feature discretization (e.g. bucketed recency) to further reduce bias in specific user segments.

By moving from ad‑hoc jitter to a principled MNAR imputation approach, we correct the statistical distortion of truncated intervals, align feature engineering with true user behavior, and equip our ML pipeline with transparent, quantifiable uncertainty.

