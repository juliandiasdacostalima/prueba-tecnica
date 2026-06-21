import code
import os
import sys
from pathlib import Path

# ── Windows / venv setup (idéntico a main.py) ────────────────────────────────
ROOT = Path(__file__).parent.resolve()

os.environ.pop("SPARK_HOME", None)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

if sys.platform == "win32":
    winutils_home = ROOT / "winutils"
    if "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = str(winutils_home)
    winutils_bin = str(winutils_home / "bin")
    path = os.environ.get("PATH", "")
    if winutils_bin not in path:
        os.environ["PATH"] = winutils_bin + os.pathsep + path

if "JAVA_HOME" not in os.environ:
    raise EnvironmentError(
        "JAVA_HOME is not set. Install JDK 11 o 17 y ejecuta:\n"
        '  setx JAVA_HOME "C:\\Program Files\\Java\\jdk-17" /M'
    )

# ── Imports Spark / Delta ─────────────────────────────────────────────────────
from delta import configure_spark_with_delta_pip   # noqa: E402
from delta.tables import DeltaTable                # noqa: E402
from pyspark.sql import SparkSession               # noqa: E402
import pyspark.sql.functions as F                  # noqa: E402

# ── Sesión ────────────────────────────────────────────────────────────────────
local_tmp = str(ROOT / "tmp").replace("\\", "/")
builder = (
    SparkSession.builder
    .appName("pyspark-shell")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.local.dir", local_tmp)
    .config("spark.driver.memory", "2g")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Rutas útiles pre-calculadas
delta_base  = str(ROOT / "data" / "delta").replace("\\", "/")
output_last = str(ROOT / "data" / "output" / "opendata_demo" / "last").replace("\\", "/")
output_hist = str(ROOT / "data" / "output" / "opendata_demo" / "historic").replace("\\", "/")

# ── Banner ────────────────────────────────────────────────────────────────────
BANNER = f"""
╔══════════════════════════════════════════════════════════╗
║              PySpark Shell  –  Delta Lake                ║
╚══════════════════════════════════════════════════════════╝

Variables disponibles:
  spark        SparkSession activa
  F            pyspark.sql.functions
  DeltaTable   delta.tables.DeltaTable
  ROOT         {ROOT}
  delta_base   {delta_base}
  output_last  {output_last}
  output_hist  {output_hist}

Ejemplos rápidos:
  spark.read.format("delta").load(delta_base + "/opendata_demo").show()
  spark.read.format("delta").load(delta_base + "/raw_opendata_demo").show()
  spark.read.format("json").load(output_last).show()
  spark.read.format("json").load(output_hist).show()
  DeltaTable.forPath(spark, delta_base + "/opendata_demo").history().show()

Escribe exit() o Ctrl-Z para salir.
"""

# ── REPL interactivo ──────────────────────────────────────────────────────────
code.interact(banner=BANNER, local=globals(), exitmsg="SparkSession cerrada.")
spark.stop()
