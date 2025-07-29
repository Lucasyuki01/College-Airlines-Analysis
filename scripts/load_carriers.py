import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv
from pathlib import Path
import os

def load_carriers(csv_path):
    # 1. Read CSV
    df = pd.read_csv(csv_path)

    # 2. Cleaning
    df["Code"] =     df["Code"].astype(str).str.strip()
    df["Description"] = df["Description"].astype(str).str.strip()
    df = df.dropna(subset=["Code"])  #Remove rows with NULL primary key

    # 3. Connect to database
    env_path = Path("secrects.env")
    load_dotenv(dotenv_path=env_path)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    cursor = conn.cursor()

    # 4. Clean table
    cursor.execute("TRUNCATE TABLE carriers;")
    conn.commit()

    # 5. COPY inserction
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    cursor.copy_expert("COPY carriers FROM STDIN WITH CSV", csv_buffer)
    conn.commit()

    cursor.close()
    conn.close()
    print("Importação de carriers.csv concluída com sucesso.")

if __name__ == "__main__":
    load_carriers("dataverse_files/carriers.csv")
