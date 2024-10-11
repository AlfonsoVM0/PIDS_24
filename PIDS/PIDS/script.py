import psycopg2
from datetime import datetime

def connect_to_db():
    return psycopg2.connect(
        host="localhost",          # Docker service name for PostgreSQL
        database="airflow",       # Database name
        user="airflow",           # Database user
        password="airflow"        # Database password
    )

connection = connect_to_db()
cursor = connection.cursor()
cursor.execute('SELECT "avg_trip_duration" FROM "analysis_results" ORDER BY "analysis_date" DESC LIMIT 1;')
result = cursor.fetchone()
