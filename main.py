import argparse
import json
import os
import sys
from pathlib import Path

# ── Windows / venv setup (must happen before any PySpark import) ──────────────
ROOT = Path(__file__).parent.resolve()

# A system-level SPARK_HOME pointing to an older standalone Spark installation
# causes pyspark to load mismatched JARs instead of its own bundled ones.
# Clearing it forces pyspark to use the JARs that ship with the pip package.
os.environ.pop("SPARK_HOME", None)

# Point PySpark to the active interpreter so it works inside a venv
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

if sys.platform == "win32":
    # winutils.exe and hadoop.dll must be placed in <project>/winutils/bin/
    # Download from: https://github.com/cdarlint/winutils (choose the Hadoop 3.x folder)
    winutils_home = ROOT / "winutils"
    if "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = str(winutils_home)
    winutils_bin = str(winutils_home / "bin")
    path = os.environ.get("PATH", "")
    if winutils_bin not in path:
        os.environ["PATH"] = winutils_bin + os.pathsep + path

if "JAVA_HOME" not in os.environ:
    raise EnvironmentError(
        "JAVA_HOME is not set. Install JDK 11 or 17 and set JAVA_HOME to its "
        "installation directory (e.g. C:\\Program Files\\Java\\jdk-17).\n"
        "You can set it permanently with: setx JAVA_HOME \"<path>\" /M"
    )

# ── PySpark / Delta imports ───────────────────────────────────────────────────
from delta import configure_spark_with_delta_pip          # noqa: E402
from delta.tables import DeltaTable                        # noqa: E402
from pyspark.sql import SparkSession                       # noqa: E402
import pyspark.sql.functions as F                         # noqa: E402


# ── Path helpers ──────────────────────────────────────────────────────────────

def resolve_path(metadata_path: str) -> str:
    """Convert metadata paths (Unix-style, leading /) to absolute local paths.

    Spark on Windows is happiest with forward-slash paths, so we normalise here.
    """
    relative = metadata_path.lstrip("/")
    return str(ROOT / relative).replace("\\", "/")


def apply_params(text: str, params: dict) -> str:
    """Replace {{ key }} placeholders with their runtime values."""
    for key, val in params.items():
        text = text.replace("{{ " + key + " }}", str(val))
        text = text.replace("{{" + key + "}}", str(val))
    return text


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark(app_name: str) -> SparkSession:
    local_tmp = str(ROOT / "tmp").replace("\\", "/")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.local.dir", local_tmp)
        .config("spark.driver.memory", "2g")
        # Silence noisy Delta Lake INFO logs
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Pipeline stages ───────────────────────────────────────────────────────────

def read_inputs(spark: SparkSession, inputs_cfg: list, params: dict) -> dict:
    dataframes: dict = {}
    for inp in inputs_cfg:
        name = inp["name"]
        cfg = inp["config"]
        path = resolve_path(apply_params(cfg["path"], params))
        fmt = cfg["format"]

        reader = spark.read.format(fmt)
        for k, v in inp.get("spark_options", {}).items():
            reader = reader.option(k, v)

        print(f"  [input]     {name}  <-  {path}")
        dataframes[name] = reader.load(path)

    return dataframes


def apply_transformations(dataframes: dict, transformations_cfg: list) -> dict:
    for t in transformations_cfg:
        name = t["name"]
        ttype = t["type"]
        src = t["input"]
        cfg = t["config"]
        df = dataframes[src]

        if ttype == "add_fields":
            for field in cfg["fields"]:
                df = df.withColumn(field["name"], F.expr(field["expression"]))

        elif ttype == "filter":
            df = df.filter(cfg["filter"])

        else:
            raise ValueError(f"Unsupported transformation type: '{ttype}'")

        print(f"  [transform] {name}  ({ttype})  <-  {src}")
        dataframes[name] = df

    return dataframes


def write_outputs(dataframes: dict, outputs_cfg: list, spark: SparkSession) -> None:
    delta_base = str(ROOT / "data" / "delta").replace("\\", "/")

    for out in outputs_cfg:
        name = out["name"]
        otype = out["type"]
        src = out["input"]
        cfg = out["config"]
        df = dataframes[src]

        if otype == "file":
            path = resolve_path(cfg["path"])
            fmt = cfg["format"]
            mode = cfg["save_mode"]
            writer = df.write.mode(mode).format(fmt)
            if "partition" in cfg:
                writer = writer.partitionBy(cfg["partition"])
            print(f"  [output]    {name}  ->  file/{fmt} ({mode})  @  {path}")
            writer.save(path)

        elif otype == "delta":
            table = cfg["table"]
            table_path = f"{delta_base}/{table}"
            mode = cfg["save_mode"]

            if mode == "merge":
                pk = cfg["primary_key"]
                condition = " AND ".join(f"e.{k} = u.{k}" for k in pk)

                if DeltaTable.isDeltaTable(spark, table_path):
                    print(f"  [output]    {name}  ->  delta merge  @  {table_path}")
                    (
                        DeltaTable.forPath(spark, table_path).alias("e")
                        .merge(df.alias("u"), condition)
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                else:
                    # First load: create the table
                    print(f"  [output]    {name}  ->  delta create (first run)  @  {table_path}")
                    df.write.format("delta").mode("overwrite").save(table_path)

            elif mode == "append":
                print(f"  [output]    {name}  ->  delta append  @  {table_path}")
                df.write.format("delta").mode("append").save(table_path)

            else:
                raise ValueError(f"Unsupported delta save_mode: '{mode}'")

        else:
            raise ValueError(f"Unsupported output type: '{otype}'")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Metadata-driven PySpark pipeline")
    parser.add_argument("--year", required=True, help="Year to process, e.g. 2024")
    args = parser.parse_args()
    params = {"year": args.year}

    metadata_path = ROOT / "metadata.json"
    with open(metadata_path, "r", encoding="utf-8") as fh:
        metadata = json.load(fh)

    spark = build_spark("metadata-driven-pipeline")
    spark.sparkContext.setLogLevel("WARN")

    try:
        for dataflow in metadata["dataflows"]:
            print(f"\n{'='*60}")
            print(f"  Dataflow: {dataflow['name']}  |  year={args.year}")
            print(f"{'='*60}")

            dfs = read_inputs(spark, dataflow["inputs"], params)
            dfs = apply_transformations(dfs, dataflow["transformations"])
            write_outputs(dfs, dataflow["outputs"], spark)

            print(f"\n  Dataflow '{dataflow['name']}' completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
