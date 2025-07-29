import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv
from pathlib import Path
import os


def load_airports(csv_path):
    # 1. Read CSV
    print('1')
    df = pd.read_csv(csv_path)

    # 2. Cleaning
    print('2')
    for col in ["iata", "airport", "city", "state", "country"]:
        df[col] = df[col].astype(str).str.strip()

    df = df.dropna(subset=["iata"])  #Remove rows with NULL primary key

    # 3. Database connection
    print('3')
    env_path = Path("secrects.env")
    load_dotenv(dotenv_path=env_path)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    cursor = conn.cursor()

    # 4. Table cleaning before insertion
    print('4')
    cursor.execute("TRUNCATE TABLE airports CASCADE;")
    conn.commit()

    # 5. Insertion with COPY
    print('5')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    cursor.copy_expert("COPY airports FROM STDIN WITH CSV", csv_buffer)
    conn.commit()

    cursor.close()
    conn.close()
    print("Importação de airports.csv concluída com sucesso.")

if __name__ == "__main__":
    load_airports("dataverse_files/airports.csv")