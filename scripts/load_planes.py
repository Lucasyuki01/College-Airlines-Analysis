import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv
import os

def load_plane_data(csv_path):
    # 1. Ler o CSV
    df = pd.read_csv(csv_path)

    # 2. Limpeza básica
    df["tailnum"] = df["tailnum"].astype(str).str.strip()
    for col in ["type", "manufacturer", "issue_date", "model", "status", "aircraft_type", "engine_type"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df = df.dropna(subset=["tailnum"])  # Remover registros sem chave primária

    # 3. Conectar ao banco
    load_dotenv(dotenv_path="secrets.env")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    cursor = conn.cursor()

    # 4. Limpar tabela antes de inserir
    cursor.execute("TRUNCATE TABLE plane_data;")
    conn.commit()

    # 5. Inserir via COPY
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    cursor.copy_expert("COPY plane_data FROM STDIN WITH CSV", csv_buffer)
    conn.commit()

    cursor.close()
    conn.close()
    print("Importação de plane-data.csv concluída com sucesso.")

if __name__ == "__main__":
    load_plane_data("data/plane-data.csv")
