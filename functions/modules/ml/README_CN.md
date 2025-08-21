# InsightFlow / ML 模块说明

本文档总结 `functions/modules/ml/` 中主要脚本的用途、输入输出、业务流程与产物位置，便于团队协作与下游（BI/营销/LLM 回调）对接。

## 1. 目录与产物（S3）

所有结果默认写入 **curated bucket**：

```
s3://<curated_bucket>/recsys/
├─ user_seg/
│  ├─ run-TS/user_seg.parquet/                  # 用户级分群（R/F/M、segment）
│  └─ latest/user_seg.parquet/
│
├─ segment_popularity/
│  ├─ run-TS/segment_popularity.parquet/        # 分群×商品 Top-N 热度/排行
│  └─ latest/segment_popularity.parquet/
│
├─ eval/                                        # ALS/基线/后排序 的原子评估指标
│  ├─ als-TS/metrics.json, coverage.json
│  └─ als-postrank-TS/metrics.json, coverage.json
│
├─ eval_charts/
│  ├─ publish-TS/                               # 指标趋势/对比图（发布）
│  │   ├─ map_trend.png, ndcg_trend.png, precision_trend.png, recall_trend.png
│  │   ├─ coverage_item_trend.png, coverage_user_trend.png, metrics_bar.png
│  │   └─ index.json
│  ├─ latest/index.json
│  └─ rfm_analysis-TS/                          # RFM 画像发布
│      ├─ cluster_profiles.png, pca_segments.png, seg_pop_lift.png
│      └─ index.json
│
├─ exports/
│  └─ run-TS/data/                              # RFM 小样本与辅助表
│      ├─ overview.csv
│      ├─ centroids.parquet                     # 各簇画像中心（segment 粒度聚合）
│      ├─ segment_topn.parquet                  # 各簇 Top-K 商品（从 Top-N 收窄）
│      ├─ users_high_value.parquet              # 高价值样本（hv=0.4*r_z+0.3*f_z+0.3*m_z）
│      ├─ users_random.parquet                  # 随机样本
│      ├─ user_recos.csv / user_recos.parquet   # 样本用户×Top-K
│      └─ bedrock_inputs.jsonl                  # LLM/Bedrock 回调输入
│
└─ debug/
   ├─ spark-events/user-seg-TS/                 # Spark 事件日志
   └─ user-seg-TS/diag/                         # 轻量诊断（driver_log.txt / metrics.json / orders_schema.json）

```

---

## 2. 脚本一览

### A. MICE 缺失值填补

- `mice_imputer.py`
  - **作用**：对订单/明细等关键字段进行缺失值推断与填补（Multiple Imputation by Chained Equations）。
  - **输入**：清洗后的原始表（clean bucket）。
  - **输出**：`after-MICE/run-TS/...`（例如 `orders_imputed.parquet`），供后续评分/训练使用。
- `mice_processing.py`
  - **作用**：MICE 前后数据的预处理与一致性保障（字段类型、取值范围、ID 对齐）。

### B. MICE 效果检查（可选但推荐）

- `compute_last_order_share.py`
  - **作用**：计算 impute 前/后在“最近30天”维度的贡献占比、稳定性等指标（如最后一次下单占比提升）。
  - **输出**：统计表（S3/本地），供可视化脚本读取。
- `plot_mice_day30.py`
  - **作用**：将上一步统计可视化（折线/柱状），便于验证 MICE 改进效果。

### C. 构建评分（供 ALS 用）

- `build_ratings.py`
  - **作用**：把 orders + order_products_* 转换为用户-商品交互评分（隐式反馈次数/加权）。
  - **输出**：`ratings.parquet/`（用户×商品×score×时间等）。
- `build_ratings_job.py`
  - **作用**：作业版封装（SageMaker Processing），与 `build_ratings.py` 同逻辑但面向集群运行与 S3。

### D. ALS 训练

- `train_als.py`、`als_train.py`
  - **作用**：训练隐语义模型（ALS），产出模型文件/超参快照（如果配置有）；或在某些实现中直接产出推荐候选。

### E. ALS 评估（基线对比）

- `als_eval.py`、`eval_als_job.py`
  - **作用**：在验证集上评估 ALS 相对 baseline 的 `precision@K/recall@K/MAP@K/NDCG@K` 与覆盖率（item/user）。
  - **输出**：`recsys/eval/als-TS/{metrics.json, coverage.json}`。
    - `metrics.json`：`{"als": {...}, "baseline": {...}, "lift": {...}, "finished_at_utc": ...}`
    - `coverage.json`：`{"coverage_item": x.x, "coverage_user_eval": y.y}`

### F. 后排序（热度混排 / 回退 + MMR 多样性）

- `als_postrank.py`、`postrank_job.py`
  - **作用**：对 ALS 候选进行 rerank：
    - 热度混排与回退（防冷启动、数据稀疏时兜底）
    - MMR 多样性（相关性/多样性加权）
  - **输出**：`recsys/eval/als-postrank-TS/{metrics.json, coverage.json}`

### G. 发布评估图表（多 run 趋势与对比）

- `publish_eval_charts.py`（最终版）
  - **作用**：扫描 `recsys/eval/`，**按 `finished_at_utc` 排序（带回退）**，汇总并发布：
    - `map_trend.png` / `ndcg_trend.png` / `precision_trend.png` / `recall_trend.png`
    - `coverage_item_trend.png` / `coverage_user_trend.png` / `metrics_bar.png`
  - **输出**：`recsys/eval_charts/publish-TS/` 与 `latest/index.json`

### H. 用户分群（RFM/KMeans）与分群热度

- `user_seg_rfm_kmeans.py`（Spark 作业）
  - **作用**：计算 `R/F/M`、标准化（`r_z/f_z/m_z`）、KMeans 分群，得到 `user_seg.parquet/`；并计算每簇的商品热度（Top-N）写出 `segment_popularity.parquet/`。
- `user_seg_job.py`
  - **作用**：SageMaker Processing 封装，负责拉起 Spark 作业；支持 `--skip_seg_pop`。

### I. RFM 可视化与小样本导出

- `rfm_analysis_and_export.py`（最终版）
  - **作用**：读取 `user_seg.latest` 与 `segment_popularity.latest`，发布 RFM 图表，并导出两组小样本（高价值、随机）及其簇 Top-K 推荐，生成 LLM/EDM 友好格式。
  - **图表**：`cluster_profiles.png`、`pca_segments.png`、`seg_pop_lift.png` → `recsys/eval_charts/rfm_analysis-TS/`
  - **样本**：详见上方 `exports/run-TS/data/` 清单

---

## 3. 推荐路径与权限
- 建议把**分析/评估/导出产物**统一落在 **curated bucket**，clean 用于“清洗后的原始域”。  
- `latest/` 作为稳定入口，历史留存到 `run-TS/`，便于回溯。

---

## 4. 版本与兼容性提示

- `awswrangler` 版本差异：**二进制上传请用 boto3.put_object**，或 `wr.s3.upload(local_file=...)`；不要传 `body=`（避免你之前遇到的参数不兼容）。  
- SageMaker SDK 与 SparkProcessor/PySparkProcessor：以当前可用类为准，避免版本符号不匹配。 
- 日志/诊断：CloudWatch 与 VPC Endpoint 未通时，脚本会把必要诊断落到 `recsys/debug/.../diag/`。

---

## 5. 典型运行命令
```bash
# 发布评估图表 （从 eval/ 汇总趋势）
python functions/modules/ml/publish_eval_charts.py --config configs/recsys.yaml

# RFM 可视化 + 小样本导出
python functions/modules/ml/rfm_analysis_and_export.py \
  --config configs/recsys.yaml \
  --strict_parquet --pca_sample 50000 \
  --hv_n 150 --rand_n 150 --topn_per_segment 5 --seed 42