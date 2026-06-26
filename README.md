# yck_wash_model

二手车数据清洗与车型库同步系统。负责把各平台的二手车爬虫数据清洗、标准化，并将车型库增量同步到 IT 生产库。

## 数据流

```
爬虫库 (DB_LOCAL)                ODS 中间层 (DB_ODS)            IT 生产库 (DB_IT)
yck-data-center  ──vdatabase_sync──▶  yck_ods  ──autohome_match──▶  yck_car_basic_*
config_autohome_*                  清洗后的车型配置              品牌/车系/车型/新能源

二手车平台原始数据  ──wash_platform──▶  分析宽表 analysis_wide_table_new
```

## 功能模块

| 模块 | 功能 |
|------|------|
| [`vdatabase_sync/`](vdatabase_sync/) | 车型库源数据同步：从爬虫库清洗汽车之家车型主表/详细配置/新能源参数，写入 ODS 层（含品牌/车系 ID 生成、变速箱/排放/排量等字段标准化） |
| [`autohome_match/`](autohome_match/) | 车型库同步：把 ODS 的品牌/车系/车型/新能源数据增量同步到 IT 生产库，以生产库为唯一基准做差集去重 |
| [`else_sync/`](else_sync/) | 其它数据同步：投诉/经销商报价/汽车销量/口碑/经销商信息/车主裸车价，按当天日期自动决定执行哪些子任务 |
| [`wash_platform/`](wash_platform/) | 二手车平台清洗：瓜子、人人车、58、车168、易车、优信、天天拍等平台的在售车源数据清洗，标准化后写入分析宽表 |
| [`vdatabase/`](vdatabase/) | YCK 标准车型库维护：品牌/车系/车型 ID 自增 |
| [`plat_id_match/`](plat_id_match/) | 平台 ID 匹配：各平台车型 ID 与 YCK 标准车型 ID 多轮迭代匹配 |
| [`common/`](common/) | 公共层：数据库连接封装、配置、邮件告警、日志 |
| [`tools/`](tools/README.md) | 运维/补数脚本（按 ID 重刷、预览、建库等），详见 [tools/README.md](tools/README.md) |

## 调度（Airflow DAG）

由 Airflow 调度，DAG 定义见 [`dags/`](dags/)：

| DAG | 调度 | 内容 |
|-----|------|------|
| `vehicle_sync` | 每周四 02:00 | `vdatabase_sync` → `autohome_match`（后者仅在前者成功后跑） |
| `else_sync` | 每天 03:00 | 内部按日期自判（每月 1/24/28 日、周四各跑不同子任务） |

所有 DAG 共享统一的重试（2 次）、超时（30min）、失败邮件告警，配置见 [`dags/_common.py`](dags/_common.py)。

## 配置

数据库密码等敏感信息一律从环境变量 / `.env` 读取，**不在代码中硬编码**（见 [`common/config.py`](common/config.py)）。

涉及的数据库：
- `DB_LOCAL` — 爬虫库（数据源）
- `DB_ODS` — ODS 中间层（ETL 结果）
- `DB_IT` — IT 云端生产库（最终写入目标）
- `DB_YUN` — 数据中台（部分数据同步目标）
- `DB_VDB` — 通用车型库

本地开发：复制 `.env.example` 为 `.env` 并填入真实值。

## 技术栈

Python 3.12 · pandas · PyMySQL · Airflow 2.10
