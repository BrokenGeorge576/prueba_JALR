import pandas as pd
from fastavro import reader
from sqlalchemy import create_engine

DB_URL = "postgresql://admin:secretpassword@postgres_db:5432/tech_test_db"


def run_loading():
    print("Iniciando carga de datos a PostgreSQL...")
    engine = create_engine(DB_URL)

    try:
        with open("data.avro", "rb") as fo:
            records = list(reader(fo))
        df = pd.DataFrame(records)
    except FileNotFoundError:
        print("Error: No se encontró data.avro. Ejecuta la extracción primero.")
        return

    df_companies = df[["company_id", "company_name"]].drop_duplicates(
        subset=["company_id"]
    )  # type: ignore
    df_companies.columns = ["id", "name"]

    try:
        df_companies.to_sql("companies", engine, if_exists="append", index=False)
        print("Tabla 'companies' cargada.")
    except Exception as e:
        print(f"Nota en 'companies': {e}")

    df_charges = df.drop(columns=["company_name"])
    df_charges["created_at"] = pd.to_datetime(df_charges["created_at"], unit="ms")
    df_charges["updated_at"] = pd.to_datetime(df_charges["updated_at"], unit="ms")

    try:
        df_charges.to_sql("charges", engine, if_exists="append", index=False)
        print("Tabla 'charges' cargada con éxito.")
    except Exception as e:
        print(f"Error al cargar 'charges': {e}")


if __name__ == "__main__":
    run_loading()
