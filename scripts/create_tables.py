import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import os

def create_tables():
    env_path = Path("secrects.env")
    load_dotenv(dotenv_path=env_path)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    cursor = conn.cursor()

    # Flights table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flights_2007 (
            Year INT,
            Month INT,
            DayofMonth INT,
            DayOfWeek INT,
            DepTime INT,
            CRSDepTime INT,
            ArrTime INT,
            CRSArrTime INT,
            UniqueCarrier TEXT,
            FlightNum INT,
            TailNum TEXT,
            ActualElapsedTime INT,
            CRSElapsedTime INT,
            AirTime INT,
            ArrDelay INT,
            DepDelay INT,
            Origin TEXT,
            Dest TEXT,
            Distance INT,
            TaxiIn INT,
            TaxiOut INT,
            Cancelled INT,
            CancellationCode TEXT,
            Diverted INT,
            CarrierDelay FLOAT,
            WeatherDelay FLOAT,
            NASDelay FLOAT,
            SecurityDelay FLOAT,
            LateAircraftDelay FLOAT
        );
    """)

    # Airports table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS airports (
            iata TEXT PRIMARY KEY,
            airport TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            lat FLOAT,
            long FLOAT
        );
    """)

    # Carriers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS carriers (
            Code TEXT PRIMARY KEY,
            Description TEXT
        );
    """)

    # Planes table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plane_data (
            tailnum TEXT PRIMARY KEY,
            type TEXT,
            manufacturer TEXT,
            issue_date TEXT,
            model TEXT,
            status TEXT,
            aircraft_type TEXT,
            engine_type TEXT,
            year INT
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully.")

if __name__ == "__main__":
    create_tables()
