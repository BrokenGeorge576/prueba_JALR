import pandas as pd

from etl.extract import transform_data


def test_transform_data_cleans_amounts_and_dates():
    mock_data = {
        "id": ["1", "2", None],
        "name": ["Empresa A", "Empresa B", "Empresa C"],
        "company_id": ["C1", "C2", "C3"],
        "amount": ["100.50", "letras_error", None],
        "status": ["paid", "failed", "paid"],
        "created_at": [
            "2023-01-01T10:00:00Z",
            "2023-01-02T10:00:00Z",
            "2023-01-03T10:00:00Z",
        ],
        "paid_at": ["2023-01-01T12:00:00Z", None, None],
    }
    df_in = pd.DataFrame(mock_data)
    df_out = transform_data(df_in)

    assert len(df_out) == 2, "Debería haber eliminado la fila con ID nulo"
    assert "company_name" in df_out.columns, "Falta renombrar 'name' a 'company_name'"
    assert "updated_at" in df_out.columns, "Falta renombrar 'paid_at' a 'updated_at'"
    amounts = df_out["amount"].tolist()
    assert amounts[0] == 100.50
    assert amounts[1] == 0.0
