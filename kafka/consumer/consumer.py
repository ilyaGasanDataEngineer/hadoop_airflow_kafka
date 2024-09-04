import socket
from confluent_kafka import Consumer
from confluent_kafka.error import KafkaError, KafkaException
import sys

consumer = Consumer({
    'group.id': 'kafka-consumer',
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    'auto.offset.reset': 'earliest'  # Позволяет начинать чтение с самого начала, если нет оффсетов
})

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)  # Увеличенный таймаут
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    sys.stderr.write(f"Error: {msg.error()}\n")
                    raise KafkaException(msg.error())
            else:
                # Декодируем сообщение и выводим
                print(f"Received message: {msg.value().decode('utf-8')} from topic {msg.topic()} at partition {msg.partition()} and offset {msg.offset()}")

    except Exception as e:
        sys.stderr.write(f"Exception: {str(e)}\n")
    finally:
        # Закрываем потребитель для коммита оффсетов
        consumer.close()

basic_consume_loop(consumer, ['weather'])
