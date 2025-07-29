import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv
import os


def load_airports(csv_path):
    # 1. Ler o CSV
    df = pd.read_csv(csv_path)

    # 2. Limpeza
    for col in ["iata", "airport", "city", "state", "country"]:
        df[col] = df[col].astype(str).str.strip()

    df = df.dropna(subset=["iata"])  # Remover linhas com chave primária nula

    # 3. Conexão com o banco
    load_dotenv(dotenv_path="secrets.env")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    cursor = conn.cursor()

    # 4. Limpar a tabela antes de inserir
    cursor.execute("TRUNCATE TABLE airports;")
    conn.commit()

    # 5. Inserção com COPY
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    cursor.copy_expert("COPY airports FROM STDIN WITH CSV", csv_buffer)
    conn.commit()

    cursor.close()
    conn.close()
    print("Importação de airports.csv concluída com sucesso.")

if __name__ == "__main__":
    load_airports("data/airports.csv")  # ajuste o caminho conforme necessário
