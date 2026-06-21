# PySpark Data Pipeline & Quality Tests

Proyecto de procesamiento de datos con PySpark. Incluye un pipeline principal orientado a metadatos y dos tests de calidad de datos.

---

## Requisitos previos

| Requisito | Versión recomendada |
|---|---|
| Python | 3.9 o superior |
| Java (JDK) | 11 o 17 |
| PySpark | según `requirements.txt` |
| delta-spark | según `requirements.txt` |

### Windows
PySpark en Windows necesita los binarios de Hadoop (`winutils.exe` y `hadoop.dll`). Deben estar en la carpeta `winutils/bin/` del proyecto. Puedes descargarlos desde [cdarlint/winutils](https://github.com/cdarlint/winutils) (carpeta Hadoop 3.x).

Además, `JAVA_HOME` debe estar configurado en las variables de entorno del sistema:
```
setx JAVA_HOME "C:\Program Files\Java\jdk-17" /M
```

### Instalación de dependencias
```bash
pip install -r requirements.txt
```

---

## Pipeline principal — `main.py`

Pipeline de ingesta de datos **orientado a metadatos**: su comportamiento completo se define en `metadata.json`, sin necesidad de modificar código Python.

### Funcionamiento

1. **Lee** el fichero `metadata.json`, que define uno o más *dataflows*.
2. Cada dataflow especifica:
   - **`inputs`**: fuentes de datos a leer (CSV, Parquet, etc.) con sus opciones de Spark.
   - **`transformations`**: transformaciones a aplicar en cadena (`add_fields` para añadir columnas calculadas, `filter` para filtrar filas).
   - **`outputs`**: destinos de escritura, que pueden ser ficheros (JSON, Parquet…) o tablas **Delta Lake** (con soporte de `merge`, `append` y `overwrite`).
3. Las rutas y parámetros del `metadata.json` admiten placeholders como `{{ year }}`, que se sustituyen en tiempo de ejecución con los argumentos pasados por CLI.

### Ejecución

```bash
python main.py --year 2025
```

Esto procesará el dataflow usando los ficheros de entrada correspondientes al año indicado (p. ej. `poblacion2025.csv`).

---

## Tests de calidad de datos

Los tests están en la carpeta `tests/` y se ejecutan con **pytest + PySpark**.  
Las opciones comunes se configuran en `tests/conftest.py`.

### Parámetros disponibles por CLI

| Parámetro | Descripción | Valor por defecto |
|---|---|---|
| `--csv1` | Ruta al primer CSV (referencia) | — |
| `--csv2` | Ruta al segundo CSV (a comparar) | — |
| `--key-cols` | Columnas clave separadas por coma | — |
| `--sep` | Separador del CSV | `;` |
| `--csv` | Ruta al CSV a validar (test de consistencia) | — |

---

### Test 1 — Detección de cambios entre dos CSVs (`test_csv_diff.py`)

Compara dos versiones de un CSV y detecta qué filas cambiaron entre ellas, clasificando cada diferencia como:

- **`NUEVO`**: la fila existe en `--csv2` pero no en `--csv1`.
- **`ELIMINADO`**: la fila existe en `--csv1` pero no en `--csv2`.
- **`MODIFICADO`**: la fila existe en ambos (misma clave) pero algún valor cambió.

El test **pasa** si no hay ninguna diferencia. Si las hay, emite un aviso con el resumen por tipo y exporta el detalle completo en `diff_output/`.

**Ejecución:**
```bash
pytest tests/test_csv_diff.py \
  --csv1 data2024.csv \
  --csv2 data2025.csv \
  --key-cols "provincia,municipio,sexo"
```

**Salida en caso de errores:** `diff_output/` (CSV con el separador indicado).

---

### Test 2 — Consistencia de totales por sexo (`test_consistencia_sexo.py`)

Valida sobre un CSV en formato largo que el valor de **`Ambos sexos`** sea igual a **`Hombres` + `Mujeres`** para cada combinación `(provincia, municipio)`.

El test **pasa** si la regla se cumple en todas las filas. Si no, emite un aviso con una muestra de los registros erróneos y los exporta en `consistencia_sexo_errores/`.

El campo `total` se trata correctamente como entero con separador de miles (`.` en formato español), evitando falsos positivos.

**Ejecución:**
```bash
pytest tests/test_consistencia_sexo.py --csv data2025.csv
```

**Salida en caso de errores:** `consistencia_sexo_errores/` (CSV con columnas `provincia`, `municipio`, `ambos_sexos`, `hombres`, `mujeres`).

---

### Ejecutar los dos tests a la vez

```bash
pytest tests/ \
  --csv data2025.csv \
  --csv1 data2024.csv \
  --csv2 data2025.csv \
  --key-cols "provincia,municipio,sexo"
```

Cada test ignora los parámetros que no le corresponden.

---

## Estructura del proyecto

```
.
├── main.py                  # Pipeline principal
├── metadata.json            # Definición de dataflows (inputs, transformaciones, outputs)
├── requirements.txt
├── winutils/                # Binarios de Hadoop para Windows (no se versiona)
├── data/                    # Datos de entrada y salida (no se versiona)
├── tmp/                     # Temporales de Spark (no se versiona)
└── tests/
    ├── conftest.py           # Opciones CLI compartidas (pytest_addoption)
    ├── test_csv_diff.py      # Test de detección de cambios entre CSVs
    └── test_consistencia_sexo.py  # Test de consistencia Hombres + Mujeres = Ambos sexos
```
