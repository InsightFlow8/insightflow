下面给出一个基于 **Python + pandas + statsmodels MICE** 的端到端方案，既能在本地验证，也可打包成 AWS Glue Python Shell Job，完美衔接你们现有的 Data Ingestion 架构。

------

## 一、方案概览

1. **用 Python 而非 Spark**
   - MI 阶段的数据量（按用户聚合后）通常 <百万级，用 pandas 更方便调用成熟的 MICE 实现。
   - 最终结果写回 S3，后续再由 Glue/Spark 统一消费。
2. **分两步走**
   - **本地验证**：在本地或 dev 环境用 pandas+statsmodels 完整跑通。
   - **生产部署**：把脚本放到 `functions/modules/feature_pipeline/` 下，配合 Glue Python Shell Job，通过 Terraform & GitHub Actions 上线。
3. **主要依赖**
   - pandas, numpy, statsmodels
   - matplotlib（诊断可视化）
   - s3fs 或 boto3 + pyarrow

------

## 二、本地验证脚本

## 环境准备

1. **启动 WSL**

   ```bash
   # 在 Windows PowerShell 中
   wsl
   ```

2. **进入项目目录**

   ```bash
   cd /mnt/g/github_env/imba_data
   ```

3. **创建 Python 虚拟环境并激活**

   ```bash
   bashCopyEditpython3 -m venv .venv
   source .venv/bin/activate
   ```

4. **安装依赖**

   ```bash
   pip install pandas numpy statsmodels matplotlib seaborn jupyterlab
   ```

5. **启动 JupyterLab**

   ```bash
   jupyter lab --no-browser --ip=0.0.0.0 --port=8888
   ```

   在 Windows 浏览器打开 `http://localhost:8888`，输入终端提示的 token。

---

[Alt] 如果想使用我们之前学习使用的已有的 Spark 虚拟环境

**A.1 激活**

```bash
# 在 Windows PowerShell 中进入 WSL
wsl

# 切到你项目目录并激活 .venv
cd /mnt/g/github_env/imba_data
source .venv/bin/activate
```

你应该看到提示符前面多了 `(.venv)`

**A.2 安装额外依赖**

在同一个 `.venv` 中直接用 pip 安装即可：

```bash
pip install \
  pandas numpy \
  statsmodels \
  matplotlib seaborn \
  jupyterlab \
  pyspark findspark
```

| 包名                | 用途                          |
| ------------------- | ----------------------------- |
| pandas              | 数据加载／清洗／聚合          |
| numpy               | 数值运算                      |
| statsmodels         | MICE 多重插补                 |
| matplotlib, seaborn | 插补后诊断可视化              |
| jupyterlab          | 本地交互式 Notebook           |
| pyspark, findspark  | 如果你后面要跑 Spark 特征管道 |

> **Tip**：如果只跑 pandas + statsmodels 的 MI，后面那两包（pyspark/findspark）不是必需的，但装了也不冲突

------

**A.3 在 JupyterLab 中验证 Spark（可选）**

1. 启动 JupyterLab：

   ```bash
   jupyter lab --no-browser --ip=0.0.0.0 --port=8888
   ```

2. 浏览器打开 `http://localhost:8888`，选 **Python 3** kernel，然后在第一个 cell 里试：

   ```python
   import findspark
   findspark.init()        # 会自动发现 SPARK_HOME
   
   from pyspark.sql import SparkSession
   spark = SparkSession.builder \
       .appName("Test") \
       .master("local[*]") \
       .getOrCreate()
   
   df = spark.read.csv("orders.csv", header=True, inferSchema=True)
   df.show(3)
   ```

   如果这一步能读出几行数据，就说明你的 Spark 环境在 Notebook 里正常可用了。

---



## Notebook

## Notebook 目录结构建议

在你的 `imba_data` 目录下，新建 `local_validation.ipynb`，并按下面标题分节：

1. **导入库 & 路径**
2. **原始数据加载**
3. **数据清洗**
4. **用户级聚合 & 缺失标记**
5. **多重插补 (MICE)**
6. **插补后诊断 & 验证**
7. **结果保存**

------

### 1. 导入库 & 路径

```python
import os
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns

# 本地数据路径（WSL 下映射的 G: 盘）
DATA_DIR = "/mnt/g/github_env/imba_data"
OUTPUT_DIR = os.path.join(DATA_DIR, "processed")
os.makedirs(OUTPUT_DIR, exist_ok=True)
```

------

### 2. 原始数据加载

```python
# 加载订单主表
orders = pd.read_csv(f"{DATA_DIR}/orders.csv")
print("orders shape:", orders.shape)
orders.head()
```

如有需要，也可加载 `order_products.csv` 或 `products.csv` 进行交叉验证。

------

### 3. 数据清洗

```python
# 3.1 类型校验 & cast
orders = orders.astype({
    "order_id": int,
    "user_id": int,
    "order_number": int,
    "order_dow": int,
    "order_hour_of_day": int,
    "days_since_prior_order": float  # NA 保留为 NaN
})

# 3.2 Domain Filter
orders = orders[
    orders.eval_set.isin(["prior","train","test"]) &
    orders.order_dow.between(0,6) &
    orders.order_hour_of_day.between(0,23)
]

# 3.3 IQR 异常值剔除 (示例: days_since_prior_order)
q1, q3 = orders["days_since_prior_order"].quantile([0.25,0.75])
iqr = q3 - q1
lower, upper = q1 - 1.5*iqr, q3 + 1.5*iqr
orders = orders[
    orders.days_since_prior_order.between(lower, upper) | orders.days_since_prior_order.isna()
]

# 3.4 关键字段缺失 & 重复行剔除
orders = orders.dropna(subset=["order_id","user_id","days_since_prior_order"], how="all")
orders = orders.drop_duplicates()

print("Cleaned orders shape:", orders.shape)
```

---

### 4. 包装

#### 4.1 以下是包装后成一个独立函数，并在stats 字典记录每一步的 before/after 行数以及 IQR 边界：

```python
from typing import Tuple, Dict
import pandas as pd

def clean_orders(
    df: pd.DataFrame,
    eval_sets: list = ["prior", "train", "test"],
    iqr_k: float = 1.5
) -> Tuple[pd.DataFrame, Dict]:
    """
    对 orders 表做以下四步清洗，返回 (cleaned_df, stats)：
      1. cast types
      2. domain filter on eval_set, order_dow, order_hour_of_day
      3. IQR 异常值剔除 (days_since_prior_order)
      4. dropna & dedup

    stats 字典包含每一步的 before/after 行数以及 IQR 边界。
    """
    df0 = df.copy()
    stats = {"initial": len(df0)}

    # 1) 类型校验 & cast
    df1 = df0.astype({
        "order_id": int,
        "user_id": int,
        "order_number": int,
        "order_dow": int,
        "order_hour_of_day": int,
        "days_since_prior_order": float
    })
    stats["after_cast"] = len(df1)

    # 2) Domain Filter
    before = len(df1)
    df2 = df1[
        df1.eval_set.isin(eval_sets) &
        df1.order_dow.between(0, 6) &
        df1.order_hour_of_day.between(0, 23)
    ]
    stats["after_domain"] = len(df2)
    stats["removed_domain"] = before - stats["after_domain"]

    # 3) IQR 异常值剔除
    before = len(df2)
    q1, q3 = df2["days_since_prior_order"].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - iqr_k * iqr, q3 + iqr_k * iqr
    df3 = df2[
        df2.days_since_prior_order.between(lower, upper) |
        df2.days_since_prior_order.isna()
    ]
    stats["after_iqr"] = len(df3)
    stats["removed_iqr"] = before - stats["after_iqr"]
    stats["iqr_bounds"] = {"q1": q1, "q3": q3, "lower": lower, "upper": upper}

    # 4) 丢弃关键字段缺失 & 重复行
    before = len(df3)
    df4 = df3.dropna(subset=["order_id", "user_id", "days_since_prior_order"], how="all")
    df4 = df4.drop_duplicates().reset_index(drop=True)
    stats["after_dropna_dedup"] = len(df4)
    stats["removed_dropna_dedup"] = before - stats["after_dropna_dedup"]

    return df4, stats
```

#### 4.2 外部查看结果的 Code Chunks

在 Notebook 或脚本中，可以分步查看

```python
# 调用清洗函数
cleaned_orders, clean_stats = clean_orders(orders)
```

**1️⃣ 打印每步统计**

```python
import json
print("Cleaning stats:\n", json.dumps(clean_stats, indent=2))
```

**2️⃣ 原始 vs 清洗后行数对比**

```python
print(f"原始行数: {clean_stats['initial']} -> 清洗后: {len(cleaned_orders)}")
```

**3️⃣ 清洗后的表结构 & 缺失**

```python
cleaned_orders.info()
print("\n缺失值统计:\n", cleaned_orders.isna().sum())
```

**4️⃣ 3.1 类型校验 & cast 后**

```python
# 3.1 类型转换后：行 × 列
print(f"3.1 类型转换后：({clean_stats['after_cast']}, {len(cleaned_orders.columns)})")
# 列类型
display(cleaned_orders.dtypes)
```

**4️⃣ 3.2 Domain Filter 后**

```python
# 3.2 Domain Filter 后
removed = clean_stats['removed_domain']
print(f"3.2 Domain Filter 后：({clean_stats['after_domain']}, {len(cleaned_orders.columns)}) （剔除 {removed} 行）")
# eval_set 分布
display(cleaned_orders['eval_set'].value_counts())
# 周/小时范围
print("order_dow 范围：", cleaned_orders.order_dow.min(), "–", cleaned_orders.order_dow.max())
print("order_hour_of_day 范围：", cleaned_orders.order_hour_of_day.min(), "–", cleaned_orders.order_hour_of_day.max())
```

**4️⃣ 3.3 IQR 异常值剔除 后**

```python
removed = clean_stats['removed_iqr']
print(f"3.3 IQR 异常值剔除后：({clean_stats['after_iqr']}, {len(cleaned_orders.columns)}) （剔除 {removed} 行）")
bounds = clean_stats['iqr_bounds']
print(f"days_since_prior_order 分位数：{bounds['q1']}，{bounds['q3']}  IQR：{bounds['q3']-bounds['q1']}")
```

**4️⃣ 3.4 关键字段缺失 & 重复行剔除**

```python
removed = clean_stats['removed_dropna_dedup']
print(f"3.4 缺失 & 重复剔除后：({clean_stats['after_dropna_dedup']}, {len(cleaned_orders.columns)}) （剔除 {removed} 行）")
display(cleaned_orders.head(3))
```

**5️⃣ 关键字段分布**

```python
plt.figure(figsize=(8,4))
sns.histplot(cleaned_orders["days_since_prior_order"].dropna(), bins=30, kde=True)
plt.title("days_since_prior_order 分布")
plt.show()
```

**6️⃣ 周期字段检查**

```python
print("order_dow 分布：")
display(cleaned_orders["order_dow"].value_counts().sort_index())
print("\norder_hour_of_day 分布：")
display(cleaned_orders["order_hour_of_day"].value_counts().sort_index())
```

**7️⃣ 重复行检查**

```python
print("重复行数:", cleaned_orders.duplicated().sum())
```

这样， Notebook 里每执行一个外部独立的代码块，就能立刻展示每一步的的清洗进度和效果，既满足本地交互式验证，也方便后续将函数复制到 Glue Python Shell Job 里做运行



------

### 4. 用户级聚合 & 缺失标记

```python
# 按 user 聚合，计算 max_days, days_imp, frequency
user_imp = (
    orders
    .groupby("user_id", as_index=False)
    .agg(
        max_days     = ("days_since_prior_order", lambda s: np.nanmax(s.values)),
        frequency    = ("order_number", "max")
    )
)
# 如果 max_days==30 则我们认为原始数据被截断，将其标记为 NA
user_imp["days_imp"] = user_imp["max_days"].apply(lambda x: np.nan if x==30 else x)
user_imp = user_imp[["user_id","days_imp","frequency"]]
print("After aggregation, missing days_imp:", user_imp["days_imp"].isna().sum())
```

------

### 5. 多重插补 (MICE)

```python
# 准备数据
mi_df = user_imp[["days_imp","frequency"]].copy()
print("原始缺失数:", mi_df["days_imp"].isna().sum())

# 参数
M, MAX_IT = 7, 10

# 构造 MICEData
mice_data = MICEData(mi_df)

# 执行循环插补
imputed_list = []
for i in range(M):
    mice_data.update_all(n_iter=MAX_IT)   # ⚠️ 这里用 n_iter 而不是 maxiter
    df_imp = mice_data.data.copy()
    df_imp["impute_id"] = i
    imputed_list.append(df_imp)
    print(f"Completed imputation {i+1}/{M}")

# 合并长表
imputed_df = pd.concat(imputed_list, ignore_index=True)
print("合并后形状:", imputed_df.shape)

```



Tips: 请用以下代码 确认你的 statsmodels 版本至少是 0.9.0 或更高

```python
import statsmodels
print(statsmodels.__version__)
```

如果低于这个版本，请在你的虚拟环境里运行 

```bash
pip install --upgrade statsmodels
```



------

### 6. 插补后诊断 & 验证

#### 6.1 计算每份插补的 days_imp 均值与标准差

```python
# 计算每份插补的 days_imp 均值与标准差
stats_imp = (
    imputed_df
    .groupby("impute_id")["days_imp"]
    .agg(["mean","std"])
    .reset_index()
)
print("各插补份的统计量：")
display(stats_imp)
```



#### 6.2 插补前后分布对比 - 检查各条插补曲线是否大体跟 Observed 重合，且没有跑偏

```python
plt.figure(figsize=(8,4))

# Observed（原始非 NA）
sns.kdeplot(
    data=user_imp, x="days_imp",
    label="Observed", color="black", fill=False
)
# 每份插补
for i in stats_imp["impute_id"]:
    subset = imputed_df[imputed_df.impute_id == i]
    sns.kdeplot(
        data=subset, x="days_imp",
        label=f"Impute {i}", alpha=0.3, legend=False
    )
plt.title("Observed vs. Imputed Density")
plt.xlim(0, 30)
plt.legend()
plt.show()



plt.figure(figsize=(8,4))
# 原始（Observed）
sns.kdeplot(
    data=user_imp, x="days_imp",
    label="Observed", color="black", lw=2
)
# 合并后的插补（All Imputed）
sns.kdeplot(
    data=imputed_df, x="days_imp",
    label="All Imputed", color="steelblue", lw=2
)
plt.title("Observed vs. All Imputed Density")
plt.xlim(0, 30)
plt.xlabel("days_imp")
plt.ylabel("Density")
plt.legend()
plt.show()

```



#### 6.3 插补前后分布对比 - 检查各条插补曲线是否大体跟 Observed 重合，且没有跑偏

逐迭代记录均值轨迹 在当前的 statsmodels 版本里，MICEData 已不再暴露内部的 data_history 属性，所以直接去读它会报错。为此，要自己“手动”在每次迭代后记录你关心的统计量，来绘制收敛轨迹 下面这种方法不依赖内部的 data_history，而是在每次迭代后手动记录一下总体或缺失值的统计量（如均值、标准差），非常直观

对每一条链都做独立的迭代追踪，然后把它们画在两块面板（mean / sd）里
多链初始化
    每个 chain 都新建一个 MICEData，并且可用 md.set_seed(chain) 保证随机性可控。

单步迭代
    在最外层循环里，每条链跑 MAX_IT 次 update_all(n_iter=1)，每次完都记录 mean 和 std。

数据整形
    把 records 做成一个长格式 DataFrame，再用 melt 同时绘制两个指标（mean、std）。

FacetGrid
    col="metric" 让 mean 和 std 各占一列面板，hue="chain" 让不同链用不同颜色，estimator=None 保证每一条线都被画出来。

```python
# 1. 参数
M = 7           # 链数（imputations）
MAX_IT = 10     # 每条链的最大迭代次数

# 2. 构造空的记录列表
records = []

# 3. 对每一条链单独初始化并迭代追踪
for chain in range(M):
    # 用全局 seed 区分不同链
    np.random.seed(chain)
    
    # 重置 MICEData
    md = MICEData(user_imp[["days_imp","frequency"]].copy())
    
    # 单步迭代并记录
    for itr in range(1, MAX_IT+1):
        md.update_all(n_iter=1)
        curr = md.data["days_imp"]
        records.append({
            "chain": chain,
            "iter":  itr,
            "mean":  curr.mean(),
            "std":   curr.std()
        })

# 4. 整理成 DataFrame
trace_df = pd.DataFrame(records)

# 5. 把 mean/std 拉成长表，方便 facet 绘图
melted = trace_df.melt(
    id_vars=["chain","iter"],
    value_vars=["mean","std"],
    var_name="metric",
    value_name="value"
)

# 6. 用 Seaborn FacetGrid 分面画出两块：mean & std
g = sns.FacetGrid(
    melted, 
    col="metric", 
    sharey=False,
    height=3, 
    aspect=2
)
g.map_dataframe(
    sns.lineplot,
    x="iter", y="value", 
    hue="chain",
    palette="tab10",
    estimator=None   # 原始线条，不做汇总
)
g.add_legend(title="Chain")
g.set_axis_labels("Iteration","Value")
g.set_titles("{col_name}")
plt.subplots_adjust(top=0.8)
g.fig.suptitle("Convergence Trace by Chain (mean & std)")
plt.show()
```



#### 6.4 箱线图查看各份插补分布 - 看下中位数、上下四分位范围是否几乎重叠

```python
plt.figure(figsize=(10,4))
sns.boxplot(
    data=imputed_df, x="impute_id", y="days_imp"
)
plt.title("Boxplot of days_imp Across Imputations")
plt.xlabel("Imputation ID")
plt.ylabel("days_imp")
plt.show()
```



**检查要点**：

- Imputed 分布是否与 Observed 大体重合？
- 不同 m 的均值/方差差异是否较小（可容忍范围 e.g. <5%）？
- 若不满足，可调大 `M` 或 `MAX_IT`，或自定义 predictor matrix。

------

### 7. 结果保存

```python
# Option 1 分别保存每个 m 的插补结果
for m in range(M):
    df_out = imputed_df[imputed_df.impute_id==m].drop(columns="impute_id")
    df_out.to_parquet(f"{OUTPUT_DIR}/user_imp_imputed_m{m}.parquet", index=False)

# Option 2 保存平均插补版本
mean_imp = imputed_df.groupby("user_id")["days_imp"].mean().reset_index()
mean_imp.to_parquet(f"{OUTPUT_DIR}/user_imp_imputed_mean.parquet", index=False)

print("All files written under", OUTPUT_DIR)
```

它们的区别

| 方面               | 多份插补（M 个文件）                                         | 平均插补（1 个文件）                 |
| ------------------ | ------------------------------------------------------------ | ------------------------------------ |
| **不确定性表达**   | 保留了插补的不确定性：每份都是一个可能的数据真相             | 丢失了不确定性，只给出一个“最可能”值 |
| **最小化方差偏差** | 可用 Rubin’s rules 训练 M 个模型并汇总系数、预测和标准误，得到对方差更好的估计 | 直接使用平均值，会低估插补不确定性   |
| **实现成本**       | 需要 M 倍的训练/预测，和后续的模型融合逻辑                   | 只要一次训练/预测，简单快速          |



### 下游 ML 场景的选择建议

1. **如果要做大规模单一模型训练**（比如一个 LightGBM、XGBoost 或深度模型），并且对插补不确定性的精确量化不是重点：
   - **使用平均插补** 的那个 Parquet 最方便，特征管道里就把 `days_imp` 当成普通数值特征去训练。
2. **如果你需要估计预测结果的置信区间** 或关心插补带来的方差影响：
   - 可以对每个 `user_imp_imputed_m{i}` 分别训练一个模型，
   - 然后按 Rubin’s rules 把 M 个模型的系数/预测和方差合并（或直接 ensemble 平均预测）。
3. **折中方案**
   - 主流程用平均插补；
   - 同时计算 `days_imp` 在 M 次插补中的 **标准差**，把它作为一个额外的不确定性特征 `days_imp_std`，让模型学习“这个值补得越不稳定是不是潜在风险”。

Tips

- **直接在 Notebook 里**迭代调整 `M`、`MAX_IT`，观察分布、统计量变化；
- 如果你习惯 R Studio，也可把上述 Python 逻辑对照到 R（mice）里做，核心思路一致；
- 本地跑通后，将 `local_validation.ipynb` 与脚本推到 GitHub，CI/CD 会自动上线到 Glue。

这样，你就能在本地用真实文件、熟悉的 Notebook 环境，完成从**数据清洗**到**MI 插补**再到**诊断验证**的全链路试验。后续部署同学只需把脚本路径改成 S3、Glue Job 调度即可。祝顺利！



---

## 三、计划方案

**数据清洗**、**MICE 插补**和**诊断／监控**本质上是三个不同关注点（concern），按模块化设计最合适。

下面给出一个「端到端 + 生产化」的**Pipeline 结构建议**，以便和我们现有的 Data Ingestion 一致：

------

### 1. Pipeline 模块划分

```
.
├── functions/
│   └── modules/
│       ├── data_ingestion/            ← 已有
│       └── feature_pipeline/          ← 你的特征管道主模块
│           ├── cleaning/              ← 1. 数据清洗 Glue PySpark 脚本
│           │   ├── clean_orders.py
│           │   └── clean_orders.sh
│           ├── imputation/            ← 2. MI 插补 Glue PythonShell 脚本
│           │   ├── run_mice.py
│           │   └── run_mice.sh
│           ├── diagnostics/           ← 3. 诊断 & 指标上报脚本（或 CloudWatch 脚本）
│           │   ├── validate_mi.py
│           │   └── metrics_setup.py
│           └── batch/                 ← 4. 特征构造 Glue PySpark 脚本（原有 feature_pipeline.py）
│               ├── feature_pipeline.py
│               └── feature_pipeline.sh
├── terraform/
│   └── modules/
│       ├── data_ingestion/            ← 已有
│       ├── glue_feature_cleaning/     ← 新增：部署 cleaning Job
│       │   └── main.tf
│       ├── glue_feature_imputation/   ← 新增：部署 imputation Job
│       │   └── main.tf
│       ├── glue_feature_diagnostics/  ← 新增：部署 diagnostic Task or CloudWatch resources
│       │   └── main.tf
│       └── glue_feature_pipeline/     ← 4. 现有的 batch 特征计算
│           └── main.tf
└── .github/
    └── workflows/
        ├── deploy_cleaning.yml         ← CI/CD for cleaning
        ├── deploy_imputation.yml       ← CI/CD for MICE
        ├── deploy_diagnostics.yml      ← CI/CD for diagnostics infra
        └── deploy_feature_pipeline.yml ← CI/CD for feature_pipeline
```

------

### 2. 各模块职责与结果保存

1. **Cleaning**
   - **脚本**：`clean_orders.py`（PySpark）
   - **输出**：写入 S3 `s3://<bucket>/cleaned/orders/` （Parquet）
   - **Summary**：同时生成 cleaning summary JSON 至 `s3://<bucket>/cleaned/summaries/`
2. **Imputation (MICE)**
   - **脚本**：`run_mice.py`（Python Shell + statsmodels）
   - **输入**：`s3://<bucket>/cleaned/orders/`
   - **输出**：多份 imputed Parquet 至 `s3://<bucket>/imputed/user_imp_m*/` + 平均插补 `.../imputed_mean/`
   - **Summary**：生成 imputation summary JSON 至 `s3://<bucket>/imputed/summaries/`
3. **Diagnostics & Monitoring**
   - **脚本**：`validate_mi.py`（可选，跑额外检查并上报 CloudWatch Metrics）
   - **或** 直接在 Terraform 里定义 CloudWatch Metric Filters & Alarms，监听 summary JSON 上报或 Glue Log Metrics
   - **目标**：自动捕捉“分布漂移”、“插补失败率过高”等异常
4. **Batch Feature Calculation**
   - **脚本**：`feature_pipeline.py`（PySpark）
   - **输入**：清洗 & 插补后的数据
   - **输出**：最终特征表 `s3://<bucket>/features/.../`

------

### 3. 结果保存 & 下游消费

- **清洗结果** 和 **插补结果** 各自写到不同前缀的 S3 路径，Glue Catalog 或 Athena 注册相应表；
- **Feature Pipeline** 直接从这些表读取，生成最终特征；
- **Summary JSON** 可供开发者在 QuickSight / Athena 上查询，也可触发 Lambda 通知或 CloudWatch Alarm；

------

### 4. CI/CD 与 Terraform

- 每个 Glue Job 模块 (`glue_feature_cleaning`, `glue_feature_imputation`, `glue_feature_diagnostics`) 都对应一个 `main.tf`，参照现有 `glue_feature_pipeline` 模块：
  - 定义 Job 名、脚本路径、IAM Role、临时目录、Glue 版本等；
  - 创建 EventBridge Rule（按需定时触发）；
  - 如 diagnostics 只需创建 CloudWatch Alarms / SNS 订阅。
- GitHub Actions：为每个模块新增一条 workflow（或使用 path-based triggers 在一个大 workflow 里分阶段执行）。

------

### 5. Tips:

- **关注点分离**：清洗、插补、诊断、特征计算各司其职，易于维护和定位问题；
- **复用 Data Ingestion 模式**：剧本化的 Terraform + Glue + CI/CD 流程一致，可快速上手；
- **便于扩展**：后续如果要增加新的诊断任务，直接往 `diagnostics/` 下加脚本 & Terraform 就行；
- **监控友好**：Summary JSON + CloudWatch Alarms 实现数据质量与插补质量的自动监控，而不必再 “人工跑 Notebook”。

------



把下面内容保存为 `feature_pipeline_mi.py`，放在本地项目 `functions/modules/feature_pipeline/batch/` 目录下。

```python
#!/usr/bin/env python3
import os
import sys
import boto3
import s3fs
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

# 1. 配置 —— S3 路径 & MI 参数
S3_INPUT_PATH  = "s3://insightflow-clean-bucket/features/user_imp.parquet"
S3_OUTPUT_DIR  = "s3://insightflow-clean-bucket/features/user_imp_imputed/"
LOCAL_TEMP_DIR = "/tmp/insightflow_mi"
M      = 7      # 并行插补份数
MAX_IT = 10     # 最大迭代次数

os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

# 2. 数据加载
print(">> Loading data from S3 …")
fs = s3fs.S3FileSystem()
df = pd.read_parquet(S3_INPUT_PATH, filesystem=fs)
print(f"Loaded {len(df)} users, columns={df.columns.tolist()}")

# 3. 数据清洗 & 聚合（如还没做过）
#    假设 ingestion 已经输出 user_imp (user_id, days_imp, frequency)
#    确认类型
df = df.astype({"user_id":int, "days_imp":float, "frequency":int})
#    丢弃 frequency 缺失
df = df.dropna(subset=["frequency"])
print("After cleaning:", df.isna().sum().to_dict())

# 4. 多重插补（MICE）—— statsmodels.MICEData
print(">> Starting MICE with m=", M, "maxit=", MAX_IT)
# 4.1 建立 MICEData
mice_data = sm.imputation.mice.MICEData(df[["days_imp", "frequency"]])
mice_data.set_data(df[["days_imp", "frequency"]])  # 载入原始

# 4.2 运行多次采样
imputed_dfs = []
for m in range(M):
    mice_data.update_all(maxiter=MAX_IT)  # 迭代插补
    imp = mice_data.data.copy()
    imp["impute_id"] = m
    imputed_dfs.append(imp)
    print(f"  → Completed imputation {m+1}/{M}")

# 合并为一张长表
result = pd.concat(imputed_dfs, ignore_index=True)

# 5. 验证与诊断
print(">> Diagnostics …")

# 5.1 缺失比例 & Plots：Observed vs Imputed
fig, ax = plt.subplots()
# 原始未缺失分布
df["days_imp"].dropna().hist(bins=30, alpha=0.5, ax=ax, label="Observed")
# 每个 m 的插补分布
for m in range(M):
    result.loc[result.impute_id==m, "days_imp"].hist(
        bins=30, alpha=0.2, ax=ax)
ax.legend()
ax.set_title("Observed vs Imputed distributions")
plt.savefig(f"{LOCAL_TEMP_DIR}/dist_compare.png")

# 5.2 一致性：各 m 之间均值 & 方差
stats = result.groupby("impute_id")["days_imp"].agg(["mean","std"])
print("Per-impute stats:\n", stats)

# 5.3 内置收敛图（statsmodels 不提供，简易绘制迭代收敛轨迹示例）
#    (MICEData 中可读取 mice_data.data_history，但此处略)

# 6. 结果写回 S3
print(">> Writing imputed datasets back to S3 …")
for m, imp in enumerate(imputed_dfs):
    path = f"{S3_OUTPUT_DIR}imputed_m{m}.parquet"
    imp.drop(columns=["impute_id"]).to_parquet(path, index=False)
    print("  → Wrote", path)

# 同时写一份平均插补版
mean_imp = result.groupby("user_id")["days_imp"].mean().reset_index()
mean_imp.to_parquet(f"{S3_OUTPUT_DIR}imputed_mean.parquet", index=False)
print("All done.")
```

**说明**

- `MICEData.update_all(maxiter=…)` 中的 `maxiter` 对应 R 里的 `maxit`；
- 通过循环不同 `random_state`（可在 `MICEData` 构造时传 seed）可得到多个插补集；
- 诊断图 & 统计結果可检查是否需要调整 `M`, `maxit` 或 predictor matrix。

------



在 AWS Glue Python Shell Job 里，不能像在 Notebook 里那样用 `print`/`display` 逐步交互式地查看结果——Glue Job 是无头（headless）的，日志会统一打到 CloudWatch，而 `display` 完全不起作用。

所以我们需要把那些“分步打印/展示”替换成：

1. **结构化日志**：使用 Python 的 `logging`（Glue 默认就会把日志推到 CloudWatch）
2. **Summary 报表**：把每一步的 before/after 数量和关键统计装到一个 dict，最后以 JSON 的形式写到 S3（或者直接用 boto3 发布到 CloudWatch Metrics）
3. **Metric 发布（可选）**：用 `boto3.client("cloudwatch")` 上报自定义指标，以便监控告警



## 样例代码

```python
import logging
import json
import boto3
import os
import pandas as pd
import numpy as np

# Glue Python Shell 里，最简单的 logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# S3 参数（Glue Job 中可用 --arguments 传入）
BUCKET = os.environ.get("OUTPUT_BUCKET", "insightflow-clean-bucket")
PREFIX = os.environ.get("OUTPUT_PREFIX", "cleaning_summaries")

s3 = boto3.client("s3")

def clean_orders(orders: pd.DataFrame) -> (pd.DataFrame, dict):
    stats = {}
    stats["initial_count"] = len(orders)

    # 3.1 类型校验 & cast
    orders = orders.astype({
        "order_id": int, "user_id": int,
        "order_number": int, "order_dow": int,
        "order_hour_of_day": int,
        "days_since_prior_order": float
    })
    stats["after_cast"] = len(orders)
    logger.info(f"3.1 cast done: {stats['initial_count']} → {stats['after_cast']}")

    # 3.2 Domain Filter
    before = len(orders)
    orders = orders[
        orders.eval_set.isin(["prior","train","test"]) &
        orders.order_dow.between(0,6) &
        orders.order_hour_of_day.between(0,23)
    ]
    stats["after_domain_filter"] = len(orders)
    logger.info(f"3.2 domain filter: removed {before - stats['after_domain_filter']} rows")

    # 3.3 IQR 异常值剔除
    before = len(orders)
    q1, q3 = orders["days_since_prior_order"].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5*iqr, q3 + 1.5*iqr
    orders = orders[
        orders.days_since_prior_order.between(lower, upper) |
        orders.days_since_prior_order.isna()
    ]
    stats["after_iqr"] = len(orders)
    stats["iqr"] = {"q1": q1, "q3": q3, "lower": lower, "upper": upper}
    logger.info(f"3.3 IQR filter: removed {before - stats['after_iqr']} rows (q1={q1}, q3={q3})")

    # 3.4 丢弃关键字段缺失 & 重复
    before = len(orders)
    orders = orders.dropna(subset=["order_id","user_id","days_since_prior_order"], how="all")
    orders = orders.drop_duplicates()
    stats["after_dropna_dedup"] = len(orders)
    logger.info(f"3.4 dropna & dedup: removed {before - stats['after_dropna_dedup']} rows")

    return orders, stats

def upload_summary(stats: dict):
    """把 stats 写成 JSON 到 S3"""
    key = f"{PREFIX}/clean_summary_{pd.Timestamp.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    body = json.dumps(stats, default=str)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    logger.info(f"Uploaded cleaning summary to s3://{BUCKET}/{key}")

def main():
    # 读取 input，通常由 Glue Arguments 传入
    input_path = os.environ.get("INPUT_PATH", "s3://.../orders_cleaned.parquet")
    df = pd.read_parquet(input_path)

    cleaned_df, stats = clean_orders(df)

    # 把清洗结果写回 S3 供下游 Feature Pipeline 使用
    output_path = os.environ.get("OUTPUT_PATH", "s3://.../orders_fully_cleaned/")
    cleaned_df.to_parquet(output_path, index=False)
    logger.info(f"Wrote cleaned orders to {output_path}")

    # 上传 Summary
    upload_summary(stats)

if __name__ == "__main__":
    main()
```

### 核心改动

- **不再用 `display()`**：Glue 上不支持，必须用日志或文件
- **返回 `stats` dict**：把每步 before/after、IQR 参数收集起来
- **日志到 CloudWatch**：`logger.info` 会自动接入 Glue 的日志系统
- **Summary JSON 到 S3**：方便人工或自动化流程查看，也可供 QuickSight 可视化

------

### 监控与告警（可选）

1. 在 CloudWatch Logs Insight 里搜索 `logger.info("3.4 dropna & dedup")` 等关键日志
2. 或者用 boto3 在 `upload_summary` 里同时调用 `cloudwatch.put_metric_data`，上报 `after_domain_filter`、`after_iqr` 等指标
3. 建立 CloudWatch Alarm，如果“清洗后行数 < 某阈值”或“清洗行数变化 > X%”就告警

---

## 与现有管道对接

1. **Gl ue Python Shell Job**

   - 在 `terraform/modules/glue_feature_pipeline/main.tf` 中，改 `job_type = "PYTHON_SHELL"`，并指向上面脚本路径。

2. **依赖**

   - 在 Glue Job 的 `--extra-py-files` 中包含 `statsmodels`、`pandas` 等 wheel 包，或使用 Glue Python 3.9 自带包。

3. **CI/CD**

   - 增加 `.github/workflows/deploy_feature_pipeline.yml` Watch path 至 `feature_pipeline_mi.py`，Pipeline 触发同现有流程。

4. **本地测试**

   - 安装依赖：

     ```bash
     pip install pandas numpy statsmodels s3fs pyarrow matplotlib
     ```

   - `python feature_pipeline_mi.py`，检查 `dist_compare.png`、`user_imp_imputed/` 下文件是否正确。

------

## 可进一步优化点

- **predictorMatrix**
  - statsmodels MICEData 默认用所有其他列做预测器；如需自定义，可在构造前手动设置 `mice_data.set_predictor_matrix(mat)`。
- **并行插补**
  - 可用 multiprocessing Pool 同时启动 MICEData，不必串行循环。
- **收敛诊断**
  - 直接读取 `mice_data.data_history["days_imp"]`，画出随迭代的均值轨迹，用于判断 `maxit` 是否足够。
- **封装成函数库**
  - 把脚本中加载、插补、诊断、写出拆分成可复用函数，便于单元测试和参数化。

------

这样，理想情况就是，确认 MI 质量后再推到 GitHub，直接由现有 CI/CD 流程把它部署到 AWS Glue，确保与 Data Ingestion 架构无缝对接。