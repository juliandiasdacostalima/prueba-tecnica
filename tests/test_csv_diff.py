import warnings
from functools import reduce

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("csv_diff_test")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_sin_cambios(spark, request):
    csv1 = request.config.getoption("--csv1")
    csv2 = request.config.getoption("--csv2")
    key_cols_raw = request.config.getoption("--key-cols")
    sep = request.config.getoption("--sep")

    if not csv1 or not csv2 or not key_cols_raw:
        pytest.skip("Requiere --csv1, --csv2 y --key-cols para ejecutarse.")

    key_cols = [c.strip() for c in key_cols_raw.split(",")]

    df1 = spark.read.option("header", "true").option("sep", sep).csv(csv1)
    df2 = spark.read.option("header", "true").option("sep", sep).csv(csv2)

    all_cols = df1.columns
    non_key_cols = [c for c in all_cols if c not in key_cols]

    df1_p = df1.select([F.col(c).alias(f"old_{c}") for c in all_cols])
    df2_p = df2.select([F.col(c).alias(f"new_{c}") for c in all_cols])

    join_cond = reduce(
        lambda a, b: a & b,
        [F.col(f"old_{k}") == F.col(f"new_{k}") for k in key_cols],
    )
    joined = df1_p.join(df2_p, join_cond, "full")

    is_nuevo = reduce(
        lambda a, b: a & b,
        [F.col(f"old_{k}").isNull() for k in key_cols],
    )
    is_eliminado = reduce(
        lambda a, b: a & b,
        [F.col(f"new_{k}").isNull() for k in key_cols],
    )
    has_mod = (
        reduce(
            lambda a, b: a | b,
            [~F.col(f"old_{c}").eqNullSafe(F.col(f"new_{c}")) for c in non_key_cols],
        )
        if non_key_cols
        else F.lit(False)
    )

    changes = (
        joined
        .withColumn(
            "TIPO_CAMBIO",
            F.when(is_nuevo, "NUEVO")
             .when(is_eliminado, "ELIMINADO")
             .when(has_mod, "MODIFICADO")
             .otherwise(None),
        )
        .filter(F.col("TIPO_CAMBIO").isNotNull())
    )

    change_count = changes.count()

    if change_count > 0:
        output_path = "diff_output"
        (
            changes
            .coalesce(1)
            .write
            .option("header", "true")
            .option("sep", sep)
            .mode("overwrite")
            .csv(output_path)
        )
        key_display = [
            F.coalesce(F.col(f"old_{k}"), F.col(f"new_{k}")).alias(k) for k in key_cols
        ]
        summary = (
            changes.select("TIPO_CAMBIO", *key_display)
            .groupBy("TIPO_CAMBIO")
            .count()
            .toPandas()
            .to_string(index=False)
        )
        warnings.warn(
            f"\n{change_count} diferencias detectadas entre los CSVs.\n{summary}\n"
            f"Detalle exportado en: '{output_path}/'",
            UserWarning,
            stacklevel=2,
        )
        pytest.fail(
            f"{change_count} diferencias detectadas. Ver detalle en '{output_path}/'.",
            pytrace=False,
        )
