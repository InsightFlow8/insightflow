InsightFlow / ML End-to-End Pipeline (MICE → ALS → RFM)

This README covers all scripts under `functions/modules/ml/`, from MICE imputation, quality checks, rating construction, ALS training/evaluation, post-ranking, chart publishing, to RFM segmentation and export.

> Convention: analytical artifacts land in the **curated bucket**. Use `latest/` as stable entrypoints; keep history under `run-YYYYMMDD-HHMMSS/`.

---

## 0. Prerequisites

- Python 3.10/3.12
- Key libs: `awswrangler`, `boto3`, `pandas`, `numpy`, `matplotlib`, `scikit-learn`, `pyyaml`, `sagemaker`
- Config: `configs/recsys.yaml`
-  s3:
    curated_bucket: insightflow-dev-curated-bucket
    eval_out_base: s3://insightflow-dev-curated-bucket/recsys/eval/
- Runtime: local driver + SageMaker Processing (Spark on YARN)

---

## 1.S3 Layout (Outputs)

```
s3://<curated_bucket>/recsys/
├─ user_seg/
│  ├─ run-TS/user_seg.parquet/                  # user-level R/F/M + segment
│  └─ latest/user_seg.parquet/
│
├─ segment_popularity/
│  ├─ run-TS/segment_popularity.parquet/        # segment × item popularity (Top-N)
│  └─ latest/segment_popularity.parquet/
│
├─ eval/                                        # ALS / baseline / post-rank raw metrics
│  ├─ als-TS/{metrics.json, coverage.json}
│  └─ als-postrank-TS/{metrics.json, coverage.json}
│
├─ eval_charts/
│  ├─ publish-TS/
│  │   ├─ map_trend.png, ndcg_trend.png, precision_trend.png, recall_trend.png
│  │   ├─ coverage_item_trend.png, coverage_user_trend.png, metrics_bar.png
│  │   └─ index.json
│  ├─ latest/index.json
│  └─ rfm_analysis-TS/
│      ├─ cluster_profiles.png, pca_segments.png, seg_pop_lift.png
│      └─ index.json
│
├─ exports/
│  └─ run-TS/data/
│      ├─ overview.csv
│      ├─ centroids.parquet                     # segment-level means (cluster profiles)
│      ├─ segment_topn.parquet                  # per-segment Top-K, refined from Top-N
│      ├─ users_high_value.parquet              # hv=0.4*r_z+0.3*f_z+0.3*m_z (Top-N users)
│      ├─ users_random.parquet
│      ├─ user_recos.csv / user_recos.parquet   # cohort × Top-K items
│      └─ bedrock_inputs.jsonl                  # one JSON object per user (LLM/Bedrock)
│
└─ debug/
   ├─ spark-events/user-seg-TS/
   └─ user-seg-TS/diag/ (driver_log.txt / metrics.json / orders_schema.json)

```

---

## 2. Script Inventory & Roles

### A. MICE Imputation

- `mice_imputer.py` – MICE-based imputations on key order/line fields; outputs `after-MICE/run-TS/...`.
- `mice_processing.py` – pre-/post-processing around MICE (types, ranges, ID alignment).

### B. MICE Checks (Recommended)

- `compute_last_order_share.py` – day-30 metrics to quantify gains after imputation.
- `plot_mice_day30.py` – visualization of the above to validate improvements.

### C. Build Ratings (for ALS)

- `build_ratings.py` – transform orders + order_products_* into user–item interactions.
- `build_ratings_job.py` – job wrapper (SageMaker Processing).

### D. ALS Training

- `train_als.py`, `als_train.py` – train ALS; save model artifacts / snapshot if configured.

### E. ALS Evaluation (Baseline Comparison)

- `als_eval.py`, `eval_als_job.py` – compute `precision@K`, `recall@K`, `MAP@K`, `NDCG@K` and coverage; write to `recsys/eval/als-TS/`.

### F. Post-ranking (Popularity hybrid / fallback + MMR)

- `als_postrank.py`, `postrank_job.py` – re-rank ALS candidates using popularity hybrid/fallback and MMR diversity; write to `recsys/eval/als-postrank-TS/`.

### G. Publish Evaluation Charts

- `publish_eval_charts.py` (final) – scan `recsys/eval/`, **sort by `finished_at_utc` with fallback**, and publish:
   trends for MAP/NDCG/Precision/Recall, coverage trends, and a latest bar chart.
   Outputs under `recsys/eval_charts/publish-TS/` and `latest/index.json`.

### H. User Segmentation (RFM/KMeans) & Segment Popularity

- `user_seg_rfm_kmeans.py` – Spark app: compute R/F/M, standardize to z-scores, KMeans; write `user_seg.parquet/` and `segment_popularity.parquet/` (Top-N).
- `user_seg_job.py` – SageMaker Processing wrapper; supports `--skip_seg_pop`.

### I. RFM Visualization & Sample Export

- `rfm_analysis_and_export.py` (final) – read `user_seg.latest` & `segment_popularity.latest`, publish charts and export two cohorts (high-value, random) with per-segment Top-K for EDM/LLM.
   Charts → `recsys/eval_charts/rfm_analysis-TS/`
   Exports → `recsys/exports/run-TS/data/`

---

## 3. Recommended data placement & permissions

- Prefer storing **analysis / evaluation / export artifacts** in the **curated bucket**; reserve the *clean* bucket for “cleaned but still source-domain” data. 
- Use `latest/` as the stable entry point for downstream consumers; keep history under `run-TS/` (e.g., `run-YYYYMMDD-HHMMSS/`) for easy backfills and audits. 

---

## 4. Version & compatibility notes

- Due to `awswrangler` API differences: for **in-memory bytes** uploads use `boto3.put_object`, or use `wr.s3.upload(local_file=...)`. **Avoid** passing `body=` to `wr.s3.upload(...)` to prevent version-specific errors. 
- SageMaker SDK symbols differ across versions (e.g., `SparkProcessor` vs `PySparkProcessor`); use whichever class exists in your installed SDK to avoid import errors. 
- When CloudWatch/VPC endpoints are unavailable, scripts emit **minimal diagnostics** to `recsys/debug/.../diag/` (e.g., `driver_log.txt`, `metrics.json`, `orders_schema.json`). 

---

## 5. Typical Commands

```
# Publish evaluation charts (from eval/)
python ml/publish_eval_charts.py --config configs/recsys.yaml

# RFM visualization + sample exports
python ml/rfm_analysis_and_export.py \
  --config ml/configs/recsys.yaml \
  --strict_parquet --pca_sample 50000 \
  --hv_n 150 --rand_n 150 --topn_per_segment 5 --seed 42
```

------

