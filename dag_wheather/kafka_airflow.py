from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import psycopg2


def consume_kafka_messages(**kwargs):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'airflow-kafka-consumer',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['weather'])
    messages = []

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaError(msg.error())
            else:
                messages.append(msg.value().decode('utf-8'))
    finally:
        consumer.close()

    return messages


def process_data(**kwargs):
    ti = kwargs['ti']
    kafka_messages = ti.xcom_pull(task_ids='consume_kafka')
    if not kafka_messages:
        return

    conn = psycopg2.connect(dbname="postgres", user="postgres", password="29892989", host="localhost")
    cursor = conn.cursor()

    for message in kafka_messages:
        cursor.execute("INSERT INTO raw_weather_data (data) VALUES (%s)", (message,))

    conn.commit()
    cursor.close()
    conn.close()


def create_silver_layer(**kwargs):
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="29892989", host="localhost")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO silver_weather_data (temperature, humidity, wind_speed, weather_desc)
        SELECT 
            (data->>'temp_C')::int AS temperature,
            (data->>'humidity')::int AS humidity,
            (data->>'windspeedKmph')::int AS wind_speed,
            data->>'weatherDesc' AS weather_desc
        FROM raw_weather_data
        WHERE data IS NOT NULL
    """)

    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'retries': 1
}

with DAG('kafka_to_postgres_silver_layer', default_args=default_args, schedule_interval='0 12 * * *',
         catchup=False) as dag:
    consume_task = PythonOperator(
        task_id='consume_kafka',
        python_callable=consume_kafka_messages
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    create_silver_task = PythonOperator(
        task_id='create_silver_layer',
        python_callable=create_silver_layer
    )

    consume_task >> process_task >> create_silver_task
