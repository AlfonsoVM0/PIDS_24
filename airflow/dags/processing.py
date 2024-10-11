from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import psycopg2
import glob
import logging
import pandas as pd
from contextlib import contextmanager

# Configuración de logging para ver los mensajes en Airflow
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración para la conexión a la base de datos
DB_CONFIG = {
    'host': 'postgres',
    'database': 'taxi_data',
    'user': 'airflow',
    'password': 'airflow'
}

# Función para abrir una conexión a la base de datos de manera segura
@contextmanager
def open_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    finally:
        if conn is not None:
            conn.close()

def load_csv_to_postgres():
    csv_files = glob.glob('/opt/airflow/data/*.csv')
    if not csv_files:
        logging.info("No se encontraron archivos CSV para cargar.")
        return

    try:
        with open_db_connection() as conn:
            cursor = conn.cursor()
            for csv_file_path in csv_files:
                with open(csv_file_path, 'r') as f:
                    next(f)  # Omitir la fila de encabezado
                    cursor.copy_from(f, 'raw_data', sep=',')
                logging.info(f"Datos cargados desde {csv_file_path}")
            conn.commit()
            cursor.close()
    except Exception as e:
        logging.error(f"Error al cargar los datos: {e}")

def analyze_data():
    csv_files = glob.glob('/opt/airflow/data/*.csv')
    if not csv_files:
        logging.info("No se encontraron archivos CSV para analizar.")
        return

    try:
        with open_db_connection() as conn:
            cursor = conn.cursor()
            for csv_file_path in csv_files:
                df = pd.read_csv(csv_file_path, parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

                # Forzar los tipos de datos necesarios
                df['VendorID'] = df['VendorID'].astype(int, errors='ignore')
                df['passenger_count'] = df['passenger_count'].astype(int, errors='ignore')
                df['trip_distance'] = df['trip_distance'].astype(float, errors='ignore')
                df['RatecodeID'] = df['RatecodeID'].astype(int, errors='ignore')
                df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str, errors='ignore')
                df['PULocationID'] = df['PULocationID'].astype(int, errors='ignore')
                df['DOLocationID'] = df['DOLocationID'].astype(int, errors='ignore')
                df['payment_type'] = df['payment_type'].astype(int, errors='ignore')
                df['fare_amount'] = df['fare_amount'].astype(float, errors='ignore')
                df['extra'] = df['extra'].astype(float, errors='ignore')
                df['mta_tax'] = df['mta_tax'].astype(float, errors='ignore')
                df['tip_amount'] = df['tip_amount'].astype(float, errors='ignore')
                df['tolls_amount'] = df['tolls_amount'].astype(float, errors='ignore')
                df['improvement_surcharge'] = df['improvement_surcharge'].astype(float, errors='ignore')
                df['total_amount'] = df['total_amount'].astype(float, errors='ignore')
                df['congestion_surcharge'] = df['congestion_surcharge'].astype(float, errors='ignore')

                # Validar que no haya datos nulos después de la conversión
                df = df.dropna(subset=[
                    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                    'trip_distance', 'RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type',
                    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
                    'improvement_surcharge', 'total_amount', 'congestion_surcharge'
                ])

                # Tomar la fecha del primer registro para el análisis
                analysis_date = df['tpep_pickup_datetime'].dt.date.iloc[0]

                # Realizar cálculos y análisis
                df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
                avg_trip_duration = float(df['trip_duration'].mean())
                avg_trip_distance = float(df['trip_distance'].mean())
                max_passengers = int(df['passenger_count'].max())
                total_revenue = float(df['total_amount'].sum())
                avg_tip_amount = float(df['tip_amount'].mean())
                most_common_payment_type = int(df['payment_type'].mode()[0])
                avg_tolls_per_trip = float(df['tolls_amount'].mean())
                most_frequent_driver = int(df['VendorID'].mode()[0])
                avg_passenger_count = float(df['passenger_count'].mean())
                longest_trip = float(df['trip_distance'].max())
                highest_tip = float(df['tip_amount'].max())
                revenue_per_mile = float(total_revenue / df['trip_distance'].sum()) if df['trip_distance'].sum() > 0 else 0.0

                logging.info(f"Insertando datos de análisis para la fecha {analysis_date}")

                cursor.execute("""
                    INSERT INTO analysis_results (
                        analysis_date, avg_trip_duration, avg_trip_distance, max_passengers,
                        total_revenue, avg_tip_amount, most_common_payment_type, avg_tolls_per_trip,
                        most_frequent_driver, avg_passenger_count, longest_trip, highest_tip,
                        revenue_per_mile
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        analysis_date, avg_trip_duration, avg_trip_distance, max_passengers,
                        total_revenue, avg_tip_amount, most_common_payment_type, avg_tolls_per_trip,
                        most_frequent_driver, avg_passenger_count, longest_trip, highest_tip,
                        revenue_per_mile
                    )
                )

            conn.commit()
            cursor.close()
            logging.info(f"Datos insertados: ({analysis_date}, {avg_trip_duration}, {avg_trip_distance}, {max_passengers}, {total_revenue}, {avg_tip_amount}, {most_common_payment_type}, {avg_tolls_per_trip}, {most_frequent_driver}, {avg_passenger_count}, {longest_trip}, {highest_tip}, {revenue_per_mile})")

    except Exception as e:
        logging.error(f"Error al analizar los datos: {e}")


def delete_csv_file():
    csv_files = glob.glob('/opt/airflow/data/*.csv')
    for csv_file_path in csv_files:
        try:
            os.remove(csv_file_path)
            logging.info(f"Archivo {csv_file_path} eliminado.")
        except OSError as e:
            logging.error(f"No se pudo eliminar {csv_file_path}: {e}")

# Argumentos predeterminados del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1),
}

# Definición del DAG
dag = DAG(
    'load_csv_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Definición de las tareas
load_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag
)

analyze_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag
)

delete_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=delete_csv_file,
    dag=dag
)

# Definir dependencias entre tareas
load_task >> analyze_task >> delete_task
