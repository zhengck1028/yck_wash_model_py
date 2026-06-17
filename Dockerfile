# 基于 Airflow 官方镜像，叠加 ETL 业务依赖
# Airflow 2.10.x + Python 3.12（兼容 pandas 3.0 / numpy 2.4）
FROM apache/airflow:2.10.4-python3.12

# 业务依赖（pandas/pymysql/pypinyin/dotenv），用 airflow 用户安装
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 业务代码会通过 docker-compose 卷挂载到 /opt/airflow/project，
# 这里不 COPY 代码，便于 git pull 后无需重新 build 镜像。
