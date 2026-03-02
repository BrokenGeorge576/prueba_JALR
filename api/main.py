import argparse
import sys

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class NumberSet:
    def __init__(self):
        self.numbers = list(range(1, 101))

    def extract(self, number: int):
        """Extrae un número del conjunto con validación estricta."""
        # Validación del input (número, menor de 100)
        if not isinstance(number, int):
            raise ValueError("El valor introducido debe ser un número entero.")
        if number < 1 or number > 100:
            raise ValueError("Error: El número debe estar entre 1 y 100.")
        if number not in self.numbers:
            raise ValueError(f"Error: El número {number} ya no está en el conjunto.")

        self.numbers.remove(number)
        return True

    def calculate_missing(self) -> int:
        expected_sum = 5050  # (100 * 101) / 2
        actual_sum = sum(self.numbers)
        missing_number = expected_sum - actual_sum
        return missing_number


app = FastAPI(
    title="Missing Number API",
    description="API para encontrar el número faltante del 1 al 100",
)
conjunto_global = NumberSet()


class ExtractRequest(BaseModel):
    number: int


@app.post("/extract/")
def extract_number(req: ExtractRequest):
    try:
        conjunto_global.extract(req.number)
        return {"message": f"Número {req.number} extraído exitosamente."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/missing/")
def get_missing_number():
    missing = conjunto_global.calculate_missing()
    if missing == 0:
        return {
            "message": "El conjunto está completo. No se ha extraído ningún número aún."
        }
    return {
        "missing_number": missing,
        "message": f"Se ha calculado que el número extraído es el {missing}",
    }


def run_cli():
    parser = argparse.ArgumentParser(
        description="Extrae un número del 1 al 100 y calcula cuál falta."
    )
    parser.add_argument("numero", type=int, help="El número a extraer (1-100)")

    args = parser.parse_args()

    conjunto = NumberSet()
    print(f"[*] Conjunto inicializado con {len(conjunto.numbers)} números.")

    try:
        conjunto.extract(args.numero)
        print(
            f"[-] Método Extract ejecutado. El número {args.numero} ha sido removido."
        )

        faltante = conjunto.calculate_missing()
        print(f"[+] ¡Cálculo exitoso! El número que se extrajo fue el: {faltante}")

    except ValueError as e:
        print(e)


if __name__ == "__main__":
    run_cli()
