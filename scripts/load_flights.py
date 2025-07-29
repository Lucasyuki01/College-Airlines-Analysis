import pandas as pd
import psycopg2
from io import StringIO
import bz2
from dotenv import load_dotenv
from pathlib import Path
import os

def load_flights(bz2_path):
    # 1. Read the compressed CSV
    print("Reading file, please wait...")
    df = pd.read_csv(bz2.open(bz2_path), low_memory=False)

    # 2. Optional: strip spaces from text fields
    df["UniqueCarrier"] = df["UniqueCarrier"].astype(str).str.strip()
    df["TailNum"] = df["TailNum"].astype(str).str.strip()
    df["Origin"] = df["Origin"].astype(str).str.strip()
    df["Dest"] = df["Dest"].astype(str).str.strip()
    df["CancellationCode"] = df["CancellationCode"].astype(str).str.strip()

    # 3. Connect to the database
    env_path = Path("secrects.env")
    load_dotenv(dotenv_path=env_path)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    cursor = conn.cursor()

    # 4. Truncate the table before inserting
    print("Clearing previous data from flights_2007...")
    cursor.execute("TRUNCATE TABLE flights_2007;")
    conn.commit()

    int_columns = [
        "Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime",
        "FlightNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay",
        "Distance", "TaxiIn", "TaxiOut", "Cancelled", "Diverted"
    ]

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
    # 5. Send data using COPY from memory
    print("Loading data into PostgreSQL...")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    cursor.copy_expert("COPY flights_2007 FROM STDIN WITH CSV", csv_buffer)
    conn.commit()

    # 6. Close connection
    cursor.close()
    conn.close()
    print("✅ flights_2007 loaded successfully.")

if __name__ == "__main__":
    load_flights("dataverse_files/2007.csv.bz2")
