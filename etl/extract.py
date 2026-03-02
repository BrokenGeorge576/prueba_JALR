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
    """Filtra y transforma el DataFrame aplicando estrictas reglas de negocio."""
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

    # [ERR-009] IDs con caracteres inválidos (Ej. asteriscos)
    # Un hash válido debe ser estrictamente alfanumérico.
    mask_id_basura = (
        ~df["company_id"].astype(str).str.isalnum()
        | ~df["id"].astype(str).str.isalnum()
    )
    if mask_id_basura.any():
        cantidad = mask_id_basura.sum()
        logger.error(
            f"[ERR-009] {cantidad} registros rechazados por tener IDs corruptos o enmascarados (ej. asteriscos)."
        )
        df = df[~mask_id_basura]
        errores_totales += cantidad

    # [ERR-002]
    # Si viene el mismo ID dos veces, nos quedamos con el primero y descartamos el resto.
    duplicados = df.duplicated(subset=["id"], keep="first")
    if duplicados.any():
        cantidad = duplicados.sum()
        logger.error(
            f"[ERR-002] {cantidad} registros rechazados por tener 'id' duplicado."
        )
        df = df[~duplicados]
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

    # [ERR-008] Montos Exorbitantes (Fuera de rango en BD)
    # Límite de DECIMAL(16,2): El número absoluto debe ser menor a 10^14
    mask_monto_gigante = df["amount"].abs() >= (10**14)
    if mask_monto_gigante.any():
        cantidad = mask_monto_gigante.sum()
        logger.error(
            f"[ERR-008] {cantidad} registros rechazados por tener un monto ilógicamente grande (Numeric Overflow)."
        )
        df = df[~mask_monto_gigante]
        errores_totales += cantidad

    for col in ["created_at", "updated_at"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # [ERR-004] Fechas de creación nulas o inválidas
    mask_no_created = df["created_at"].isnull()
    if mask_no_created.any():
        cantidad = mask_no_created.sum()
        logger.error(
            f"[ERR-004] {cantidad} registros rechazados por no tener fecha 'created_at'."
        )
        df = df[~mask_no_created]
        errores_totales += cantidad

    # [ERR-005] Limpieza de Estatus Corruptos
    estatus_validos = [
        "expired",
        "partially_refunded",
        "pending_payment",
        "refunded",
        "pre_authorized",
        "charged_back",
        "voided",
        "paid",
    ]
    mask_status_invalido = ~df["status"].isin(estatus_validos)
    if mask_status_invalido.any():
        cantidad = mask_status_invalido.sum()
        logger.error(
            f"[ERR-005] {cantidad} registros rechazados por tener un 'status' corrupto o no reconocido."
        )
        df = df[~mask_status_invalido]
        errores_totales += cantidad

    # [ERR-006]
    # Si está pagado, ES OBLIGATORIO que tenga fecha de pago (updated_at)
    mask_paid_no_date = (df["status"] == "paid") & df["updated_at"].isnull()
    if mask_paid_no_date.any():
        cantidad = mask_paid_no_date.sum()
        logger.error(
            f"[ERR-006] {cantidad} registros rechazados: Status es 'paid' pero carecen de fecha de pago."
        )
        df = df[~mask_paid_no_date]
        errores_totales += cantidad

    # [ERR-007]
    # La fecha de pago/actualización NO puede ser más vieja que la fecha de creación del cargo
    mask_fechas_ilogicas = df["updated_at"].notnull() & (
        df["updated_at"] < df["created_at"]
    )
    if mask_fechas_ilogicas.any():
        cantidad = mask_fechas_ilogicas.sum()
        logger.error(
            f"[ERR-007] {cantidad} registros rechazados: La fecha de pago es anterior a la fecha de creación."
        )
        df = df[~mask_fechas_ilogicas]
        errores_totales += cantidad

    if errores_totales > 0:
        logger.warning(
            f"Calidad de Datos: Se descartaron {errores_totales} registros defectuosos de {filas_iniciales} originales."
        )
    else:
        logger.success(
            "Calidad de Datos: 100% de los registros pasaron las validaciones."
        )

    for col in ["created_at", "updated_at"]:
        df[col] = df[col].apply(
            lambda x: int(x.timestamp() * 1000) if pd.notnull(x) else None
        )

    return df


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
        df = df.replace({pd.NA: None, float("nan"): None})
        records = df.to_dict("records")
        for row in records:
            if row.get("created_at") is not None:
                row["created_at"] = int(row["created_at"])
                if row.get("updated_at") is not None:
                    row["updated_at"] = int(row["updated_at"])

        try:
            with open(AVRO_FILE, "wb") as out:
                writer(out, parsed_schema, records)
                logger.success(
                    f"Exportación exitosa. Datos guardados en '{AVRO_FILE}'."
                )
        except Exception as e:
            logger.exception(f"Error interno al escribir el archivo Avro: {e}")


if __name__ == "__main__":
    run_extraction()
