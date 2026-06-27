"""一键检查 PySpark 本地运行环境是否就绪。装好 Java17 + winutils 后运行。"""
import os, sys, shutil

ok = True
def check(name, cond, hint=""):
    global ok
    mark = "OK " if cond else "XX "
    print(f"[{mark}] {name}" + (f"  -> {hint}" if not cond and hint else ""))
    if not cond: ok = False

jh = os.environ.get("JAVA_HOME", "")
check("JAVA_HOME 已设置", bool(jh), "设为 JDK 17 目录")
hh = os.environ.get("HADOOP_HOME", "")
check("HADOOP_HOME 已设置", bool(hh), r"设为 C:\hadoop")
check("winutils.exe 存在", os.path.isfile(os.path.join(hh, "bin", "winutils.exe")) if hh else False)
check("java 命令可用", shutil.which("java") is not None)

# 真正起一个 SparkSession 跑一行
if ok:
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("envcheck").getOrCreate()
        n = spark.range(5).count()
        spark.stop()
        check(f"SparkSession 启动 + 计算 (range(5).count()={n})", n == 5)
    except Exception as e:
        check("SparkSession 启动", False, str(e)[:200])

print("\n" + ("[就绪] 环境 OK，可以跑 Spark 试点了" if ok else "[未就绪] 还有项未通过，按提示修复后重开终端再试"))
