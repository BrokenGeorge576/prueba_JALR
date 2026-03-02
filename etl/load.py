import os
import sys

import pandas as pd
from fastavro import reader
from loguru import logger
from sqlalchemy import create_engine

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

DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "secretpassword")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "tech_test_db")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def run_loading():
    logger.info("Iniciando carga de datos a PostgreSQL...")
    engine = create_engine(DB_URL)

    try:
        with open("data.avro", "rb") as fo:
            records = list(reader(fo))
        df = pd.DataFrame(records)
        logger.debug(f"Se leyeron {len(df)} registros válidos del archivo Avro.")
    except FileNotFoundError:
        logger.error(
            "Error crítico: No se encontró data.avro. Ejecuta la extracción primero."
        )
        return

    df_companies = df[["company_id", "company_name"]].drop_duplicates(
        subset=["company_id"]
    )  # type: ignore
    df_companies.columns = ["id", "name"]

    try:
        df_companies.to_sql("companies", engine, if_exists="append", index=False)
        logger.success("Catálogo 'companies' cargado exitosamente.")
    except Exception as e:
        logger.warning(f"Nota en carga de 'companies': {e}")

    df_charges = df.drop(columns=["company_name"])
    df_charges["created_at"] = pd.to_datetime(df_charges["created_at"], unit="ms")
    df_charges["updated_at"] = pd.to_datetime(df_charges["updated_at"], unit="ms")

    try:
        df_charges.to_sql("charges", engine, if_exists="append", index=False)
        logger.success("Tabla transaccional 'charges' cargada con éxito.")
    except Exception as e:
        logger.exception(f"Error crítico al cargar 'charges': {e}")


if __name__ == "__main__":
    run_loading()
