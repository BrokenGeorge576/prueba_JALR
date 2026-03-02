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
    """Filtra y transforma el DataFrame. Los datos inválidos son descartados y reportados."""
    logger.debug("Iniciando validación estricta de calidad de datos...")
    filas_iniciales = len(df)
    errores_totales = 0

    df = df.rename(columns={"name": "company_name", "paid_at": "updated_at"})

    # [ERR-001] IDs Nulos
    mask_nulos = df["id"].isnull() | df["company_id"].isnull()
    if mask_nulos.any():
        cantidad = mask_nulos.sum()
        logger.error(
            f"[ERR-001] {cantidad} registros rechazados por no tener 'id' o 'company_id'."
        )
        df = df[~mask_nulos]
        errores_totales += cantidad

    # [ERR-002] Longitud de IDs excede la Base de Datos
    mask_largo_id = df["id"].astype(str).str.len() > 24
    mask_largo_company = df["company_id"].astype(str).str.len() > 24
    mask_largos = mask_largo_id | mask_largo_company
    if mask_largos.any():
        cantidad = mask_largos.sum()
        logger.error(
            f"[ERR-002] {cantidad} registros rechazados porque sus IDs exceden los 24 caracteres permitidos."
        )
        df = df[~mask_largos]
        errores_totales += cantidad

    # [ERR-003] Montos No Numéricos
    df["amount_numeric"] = pd.to_numeric(df["amount"], errors="coerce")
    mask_monto_invalido = df["amount_numeric"].isnull() & df["amount"].notnull()
    if mask_monto_invalido.any():
        cantidad = mask_monto_invalido.sum()
        logger.error(
            f"[ERR-003] {cantidad} registros rechazados por tener texto en la columna 'amount'."
        )
        df = df[~mask_monto_invalido]
        errores_totales += cantidad

    df["amount"] = df["amount_numeric"].fillna(0.0)
    df = df.drop(columns=["amount_numeric"])

    for col in ["created_at", "updated_at"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # [ERR-004] Pagado pero sin fecha de pago
    mask_paid_no_date = (df["status"] == "paid") & df["updated_at"].isnull()
    if mask_paid_no_date.any():
        cantidad = mask_paid_no_date.sum()
        logger.error(
            f"[ERR-004] {cantidad} registros rechazados: Status es 'paid' pero carecen de fecha de pago."
        )
        df = df[~mask_paid_no_date]
        errores_totales += cantidad

    # [ERR-005] No pagado pero con fecha de pago
    mask_not_paid_with_date = (df["status"] != "paid") & df["updated_at"].notnull()
    if mask_not_paid_with_date.any():
        cantidad = mask_not_paid_with_date.sum()
        logger.error(
            f"[ERR-005] {cantidad} registros rechazados: Tienen fecha de pago, pero su status no es 'paid'."
        )
        df = df[~mask_not_paid_with_date]
        errores_totales += cantidad

    if errores_totales > 0:
        logger.warning(
            f"Calidad de Datos: Se descartaron {errores_totales} registros de {filas_iniciales} originales."
        )
    else:
        logger.success(
            "Calidad de Datos: 100% de los registros pasaron las validaciones."
        )

    for col in ["created_at", "updated_at"]:
        df[col] = df[col].apply(
            lambda x: int(x.timestamp() * 1000) if pd.notnull(x) else None
        )

    return df.replace({pd.NA: None, float("nan"): None})


def run_extraction():
    logger.info("Iniciando proceso de extracción de datos...")

    try:
        df = pd.read_csv(CSV_FILE)
        logger.debug(f"Archivo CSV leído correctamente. Filas leídas: {len(df)}")
    except FileNotFoundError:
        logger.error(f"Error crítico: No se encontró el archivo {CSV_FILE}")
        return

    df = transform_data(df)
    logger.debug(f"Datos validados. Filas a exportar: {len(df)}")

    with open(SCHEMA_FILE, "r") as f:
        schema = json.load(f)
    parsed_schema = parse_schema(schema)
    records = df.to_dict("records")

    try:
        with open(AVRO_FILE, "wb") as out:
            writer(out, parsed_schema, records)
        logger.success(f"Exportación exitosa. Datos guardados en '{AVRO_FILE}'.")
    except Exception as e:
        logger.exception(f"Error interno al escribir el archivo Avro: {e}")


if __name__ == "__main__":
    run_extraction()
