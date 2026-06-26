# tools —— 运维 / 补数脚本

车型库 ETL 的辅助工具，配合主流程（`vehicle_sync` DAG = `vdatabase_sync` + `autohome_match`）使用。
均在**项目根目录**执行，数据库连接读 `common/config.py`（即 `.env` / 环境变量）。

| 脚本 | 用途 |
|------|------|
| [`resync_autohome_by_ids.py`](resync_autohome_by_ids.py) | 按指定 autohome_id 重新解析并同步车型到 IT 生产库 |
| [`preview_autohome_match.py`](preview_autohome_match.py) | 只读预览 autohome_match 的新增/差异，导出 CSV，不写库 |
| [`create_db_yck_ods.sql`](create_db_yck_ods.sql) | 初次部署时创建 ODS 层数据库 `yck_ods` 及其表结构 |

---

## resync_autohome_by_ids.py

### 解决什么问题

主流程 `vehicle_sync` 每周四按 `add_time/update_time` **增量**同步。当某些车型在**爬虫库被修正/补全**后（典型场景：爬虫修复了字段错位等脏数据），需要**单独把这几个车型重刷到 IT 生产库**，而不必等下一次全量增量、也不影响其它车型。

本脚本就是「按 autohome_id 定点重刷」：

```
爬虫库(DB_LOCAL)  ──阶段一──▶  ODS(DB_ODS)  ──阶段二──▶  IT 生产库(DB_IT)
config_autohome_*              yck_ods                  yck_car_basic_config
                                                        yck_car_basic_config_ev
```

### 与主流程的两点区别

1. **取数范围**：把 `vdatabase_sync` / `autohome_match` 中「按时间增量」的 SQL 条件，改为 `WHERE model_id IN (指定 ID)`。所有字段清洗逻辑**直接复用**这两个 handler 的函数，不重复实现。
2. **写入策略**：对 `yck_car_basic_config` 采用**保留 id 的覆盖写入**——
   - 已存在车型：查出原自增主键 `id`，带 `id` 用 `REPLACE INTO` **原地覆盖**（`id` 不变，避免其它表通过 `id` 外键引用时悬空）；
   - 全新车型：`id` 留空，由 `AUTO_INCREMENT` 分配。
   - 新能源扩展表 `yck_car_basic_config_ev` 同样按主键 `id` 覆盖。

### 用法

```bash
# 1) 预览（只读，不写任何库）——强烈建议先跑这个
python tools/resync_autohome_by_ids.py --ids 57000,73878 --dry-run

# 2) 从文件读 ID（每行一个；支持 # 注释、逗号分隔、非整数行自动跳过）
python tools/resync_autohome_by_ids.py --id-file tools/ids.csv --skip-header --dry-run

# 3) 确认无误后正式写入
python tools/resync_autohome_by_ids.py --id-file tools/ids.csv --skip-header --commit
```

### 参数

| 参数 | 说明 |
|------|------|
| `--ids` | 逗号分隔的 autohome_id，如 `12345,67890`（手输，非整数会报错） |
| `--id-file` | ID 文件路径，每行一个；支持 `#` 注释、逗号分隔，非整数行自动跳过 |
| `--skip-header` | 跳过 `--id-file` 的第一行（表头，如 `"autohome_id"`） |
| `--dry-run` | 只读预览，不写任何库（**默认**），结果导出 `resync_preview_config_<时间戳>.csv` |
| `--commit` | 正式写入：阶段一写 ODS，阶段二保留 id 覆盖 IT |

### ⚠️ 重要：dry-run 预览的局限

dry-run **不写 ODS**，所以**阶段二预览是从 ODS 的历史存量读取的**，并不反映爬虫的最新修复：

- 对**已在 ODS 的车型**：阶段二预览准。
- 对**ODS 里还没有的新车型**：阶段二预览查不到它们（宽表会少几条），但这只是 dry-run 假象——`--commit` 时阶段一会先把它们写进 ODS，阶段二随即就能读到并正常写入。

**验证爬虫到底修没修好，应直连爬虫库（DB_LOCAL）核对源头**，而不是看阶段二预览。例如检测字段错位（错位特征：`emission` 是纯数字、`fuel` 像轮胎规格 `R\d`）：

```python
import sys; sys.path.insert(0, '.')
import pandas as pd
from common import config, db

ids = pd.read_csv('tools/ids.csv')['autohome_id'].astype(int).tolist()
in_ids = ','.join(map(str, ids))
src = db.query(config.DB_LOCAL, f"""
    SELECT autohome_id, emission, gear_box, `length`, width, fuel
    FROM config_autohome_detail_info WHERE autohome_id IN ({in_ids})""")
bad = src[src['emission'].fillna('').astype(str).str.fullmatch(r'[0-9]+')
          | src['fuel'].fillna('').astype(str).str.contains(r'R[0-9]')]
print('源头错位:', '✅ 无' if bad.empty else bad['autohome_id'].tolist())
```

### 写后校验

`--commit` 后查 IT 生产库确认数据干净、id 未变：

```python
import sys; sys.path.insert(0, '.')
import pandas as pd
from common import config, db

ids = pd.read_csv('tools/ids.csv')['autohome_id'].astype(int).tolist()
in_ids = ','.join(map(str, ids))
it = db.query(config.DB_IT, f"""
    SELECT id, autohome_id, emission, fuel
    FROM yck_car_basic_config WHERE autohome_id IN ({in_ids})""")
print('命中:', len(it), '/', len(ids))
bad = it[it['emission'].fillna('').astype(str).str.fullmatch(r'[0-9]+')]
print('残留错位:', '✅ 无' if bad.empty else bad['autohome_id'].tolist())
```

### 手动录入车型保护

`autohome_id > 1111111000`（`common/config.MANUAL_AUTOHOME_ID_THRESHOLD`）的车型是**人工直接录入 IT 生产库**的数据（不经爬虫/ODS，如冷门/老款车）。本工具会**自动剔除**清单中落在此区间的 id 并打 WARNING，绝不重刷它们，避免用自动数据覆盖手工维护内容。若整份清单都在手动区间，脚本直接报错退出。

> 主流程 `autohome_match` 的价格更新、新能源同步环节也已加同样的防御性过滤（`WHERE autohome_id <= 阈值`）。阈值可用环境变量 `MANUAL_AUTOHOME_ID_THRESHOLD` 覆盖。

### 注意事项

- **能否同步的硬性过滤**（沿用主流程逻辑）：`status != '即将销售'`、`model_year` 非空、`model_price != 0`、详细配置 `level != '-'`。不满足的 ID 会在日志打 WARNING。
- 预览 CSV（`resync_preview_config_*.csv`）是临时产物，核对后请删除，勿提交进 git。
- ID 很多（上万）时，单条 `IN (...)` 可能过长，需要时再做分批。

---

## preview_autohome_match.py

只读模拟 `autohome_match` 全流程，对比 DB_LOCAL 与 DB_IT，把**新品牌 / 新车系 / 新车型宽表 / 指导价变更**导出为 CSV，**不写任何库**。用于正式同步前核查会新增/改动哪些数据。

```bash
python tools/preview_autohome_match.py
# 产出：preview_brand.csv / preview_series.csv / preview_config.csv / preview_price_update.csv
```

---

## create_db_yck_ods.sql

初次部署时在 DB_LOCAL（ODS 服务器）上创建 `yck_ods` 库及全部表结构（车型主表/详情/新能源参数等）。仅首次部署执行一次。

```bash
mysql -h <DB_LOCAL_HOST> -u <user> -p < tools/create_db_yck_ods.sql
```
