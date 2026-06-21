import warnings

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("consistencia_sexo_test")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_ambos_sexos_igual_hombres_mas_mujeres(spark, request):
    csv_path = request.config.getoption("--csv")
    sep = request.config.getoption("--sep")

    if not csv_path:
        pytest.skip("Requiere --csv para ejecutarse.")

    df = (
        spark.read
        .option("header", "true")
        .option("sep", sep)
        .csv(csv_path)
        .withColumn("total", F.regexp_replace(F.col("total"), "\\.", "").cast("long"))
    )

    # Agrega los tres valores de sexo por (provincia, municipio)
    df_agg = df.groupBy("provincia", "municipio").agg(
        F.max(F.when(F.col("sexo") == "Ambos sexos", F.col("total"))).alias("ambos_sexos"),
        F.max(F.when(F.col("sexo") == "Hombres", F.col("total"))).alias("hombres"),
        F.max(F.when(F.col("sexo") == "Mujeres", F.col("total"))).alias("mujeres"),
    )

    errores = df_agg.filter(
        F.col("ambos_sexos").isNull()
        | F.col("hombres").isNull()
        | F.col("mujeres").isNull()
        | (F.col("ambos_sexos") != F.col("hombres") + F.col("mujeres"))
    )

    error_count = errores.count()

    if error_count > 0:
        output_path = "consistencia_sexo_errores"
        (
            errores
            .coalesce(1)
            .write
            .option("header", "true")
            .option("sep", sep)
            .mode("overwrite")
            .csv(output_path)
        )
        sample = (
            errores.select("provincia", "municipio", "ambos_sexos", "hombres", "mujeres")
            .limit(10)
            .toPandas()
            .to_string(index=False)
        )
        warnings.warn(
            f"\n{error_count} combinaciones (provincia, municipio) no cumplen la regla "
            f"'Ambos sexos = Hombres + Mujeres'.\n{sample}\n"
            f"Registros erróneos exportados en: 'consistencia_sexo_errores/'",
            UserWarning,
            stacklevel=2,
        )
        pytest.fail(
            f"{error_count} errores de consistencia detectados. "
            f"Ver detalle en 'consistencia_sexo_errores/'.",
            pytrace=False,
        )
