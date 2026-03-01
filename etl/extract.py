import json

import pandas as pd
from fastavro import parse_schema, writer

CSV_FILE = "../data/data_prueba_técnica.csv"
AVRO_FILE = "data.avro"
SCHEMA_FILE = "esquema.avsc"


def run_extraction():
    print("Iniciando extracción y limpieza de datos...")

    try:
        df = pd.read_csv(CSV_FILE)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {CSV_FILE}")
        return

    df = df.dropna(subset=["id", "company_id"])
    df = df.rename(columns={"name": "company_name", "paid_at": "updated_at"})
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["amount"] = df["amount"].fillna(0.0)

    for col in ["created_at", "updated_at"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = df[col].apply(
            lambda x: int(x.timestamp() * 1000) if pd.notnull(x) else None
        )

    df = df.replace({pd.NA: None, float("nan"): None})

    with open(SCHEMA_FILE, "r") as f:
        schema = json.load(f)
    parsed_schema = parse_schema(schema)
    records = df.to_dict("records")

    with open(AVRO_FILE, "wb") as out:
        writer(out, parsed_schema, records)

    print(
        f"Extracción exitosa. Datos validados y guardados en {AVRO_FILE} formato Avro."
    )


if __name__ == "__main__":
    run_extraction()
