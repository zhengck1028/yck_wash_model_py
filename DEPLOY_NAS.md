# 飞牛 NAS 部署 Airflow ETL

在飞牛 NAS (fnOS, Debian) 上用 Docker Compose 跑 Airflow，调度车型库 ETL。

## 前置条件

- NAS 已装 Docker + docker compose（飞牛自带）
- NAS 内网可访问业务 MySQL（爬虫库/ODS，本机 192.168.100.168）和云生产库 DB_IT
- 可用内存 ≥ 2GB

---

## 一、首次部署

### 1. SSH 进 NAS，拉代码

```bash
# 选个目录，比如 /vol1/docker
cd /vol1/docker
git clone <你的仓库地址> yck_etl
cd yck_etl
```

> 还没建远程仓库的话，先在 GitHub/Gitea 建私有库，把本地代码 push 上去，再在 NAS clone。

### 2. 建 `.env`（数据库密码 + Airflow 账号）

```bash
cp .env.example .env
vim .env   # 填入真实密码
```

`.env` 至少要有（业务库密码 + Airflow 管理员账号）：

```
DB_LOCAL_HOST=192.168.100.168
DB_LOCAL_PASS=000000
DB_ODS_HOST=192.168.100.168
DB_ODS_PASS=000000
DB_IT_HOST=47.107.191.81
DB_IT_PASS=<IT库密码>
SMTP_PASS=<邮件密码>

# Airflow Web 登录账号（自定义）
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<改成你的密码>

# 容器内文件属主（避免日志目录权限问题），取当前用户 uid
AIRFLOW_UID=1000
```

> `AIRFLOW_UID` 取你 SSH 用户的 uid：`id -u`

### 3. 建持久化目录并赋权

```bash
mkdir -p airflow_logs airflow_pgdata
# 让容器内 airflow 用户能写日志
sudo chown -R $(id -u):0 airflow_logs
```

### 4. 构建镜像 + 初始化

```bash
# 构建（装 pandas/pymysql 等业务依赖，约几分钟）
sudo docker compose build

# 初始化元数据库 + 创建管理员（跑完即退出）
sudo docker compose up airflow-init
```

看到 `airflow-init` 退出码 0、日志有 `User "admin" created` 即成功。

### 5. 启动 Airflow

```bash
sudo docker compose up -d postgres scheduler webserver
```

等约 30 秒，检查状态：

```bash
sudo docker compose ps
```

scheduler 和 webserver 都 `healthy` 即成功。

### 6. 访问 Web UI

浏览器打开 `http://192.168.100.168:8080`，用第 2 步设的账号登录。

---

## 二、验证 DAG

1. UI 里应能看到 `vehicle_sync` 和 `else_sync` 两个 DAG（默认暂停状态）
2. **先手动测**：点 `vehicle_sync` → 右上角 ▶ Trigger DAG → 看 Graph 视图，
   `vdatabase_sync_task` 跑完变绿后 `autohome_match_task` 才开始
3. 点 task → Logs 看运行日志（即我们的结构化日志）
4. 确认无误后，打开 DAG 左侧开关（unpause），让它按 cron 自动跑

### ⚠️ 容器访问数据库的网络验证

容器内能否连到 `192.168.100.168:3306`，首次要验证：

```bash
sudo docker compose exec scheduler python -c "from common import config, db; print(db.query(config.DB_LOCAL,'SELECT 1 c')['c'].iloc[0])"
```

返回 `1` 即网络通。若超时，说明 Docker bridge 网络访问宿主机 IP 受限，
改用宿主机网关 IP 或在 .env 把 DB_LOCAL_HOST/DB_ODS_HOST 设为 NAS 在
docker 网络里的网关（`host.docker.internal` 或 `172.17.0.1`）。

---

## 三、日常更新代码

本地改完代码 push，然后 NAS 上：

```bash
cd /vol1/docker/yck_etl
git pull
# DAG 改动：Airflow 自动热加载，无需重启
# 业务代码改动：scheduler 已挂载，下次 task 运行即生效
# 依赖（requirements.txt）变了才需要：
sudo docker compose build && sudo docker compose up -d
```

---

## 四、常用运维

```bash
# 看实时日志
sudo docker compose logs -f scheduler

# 重启
sudo docker compose restart scheduler webserver

# 停止（保留数据）
sudo docker compose down

# 手动补跑某天（在 UI Trigger 时填 logical date，或命令行）
sudo docker compose exec scheduler airflow dags trigger vehicle_sync
```
