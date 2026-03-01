import json
import os
import sys

import pandas as pd
from fastavro import parse_schema, writer
from loguru import logger

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL)
logger.add(
    "logs/etl_{time:YYYY-MM-DD}.log",
    rotation="10 MB",
    retention="7 days",
    serialize=True,
    level="DEBUG",
)

CSV_FILE = "../data/data_prueba_técnica.csv"
AVRO_FILE = "data.avro"
SCHEMA_FILE = "esquema.avsc"


def transform_data(df):
    """Limpia y transforma el DataFrame para cumplir con el esquema."""
    logger.debug("Iniciando transformación y limpieza de columnas...")
    df = df.dropna(subset=["id", "company_id"])
    df = df.rename(columns={"name": "company_name", "paid_at": "updated_at"})
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["amount"] = df["amount"].fillna(0.0)

    for col in ["created_at", "updated_at"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = df[col].apply(
            lambda x: int(x.timestamp() * 1000) if pd.notnull(x) else None
        )

    return df.replace({pd.NA: None, float("nan"): None})


def run_extraction():
    logger.info("Iniciando proceso de extracción de datos...")

    try:
        df = pd.read_csv(CSV_FILE)
        logger.debug(f"Archivo CSV leído correctamente. Filas iniciales: {len(df)}")
    except FileNotFoundError:
        logger.error(f"Error: No se encontró el archivo {CSV_FILE}")
        return

    df = transform_data(df)
    logger.debug(f"Datos transformados. Filas resultantes: {len(df)}")

    with open(SCHEMA_FILE, "r") as f:
        schema = json.load(f)
    parsed_schema = parse_schema(schema)
    records = df.to_dict("records")

    try:
        with open(AVRO_FILE, "wb") as out:
            writer(out, parsed_schema, records)
        logger.success(
            f"Extracción exitosa. Datos validados y guardados en '{AVRO_FILE}' formato Avro."
        )
    except Exception as e:
        logger.exception(f"Ocurrió un error al escribir el archivo Avro: {e}")


if __name__ == "__main__":
    run_extraction()
