import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from src.config import kafka_conf, TOPIC


def total_esperado(consumer: Consumer) -> int:
    """Suma de mensajes disponibles en todas las particiones."""
    md = consumer.list_topics(TOPIC, timeout=15)
    total = 0
    for pid in md.topics[TOPIC].partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, pid), timeout=10)
        total += high - low
    return total


def consume_all(idle_timeout: float = 10.0) -> pd.DataFrame:
    """Lee todo el tópico y devuelve un DataFrame con un registro por mensaje."""
    consumer = Consumer(kafka_conf())
    esperado = total_esperado(consumer)
    print(f"Mensajes esperados en el tópico: {esperado}")

    consumer.subscribe([TOPIC])
    registros, invalidos = [], 0
    try:
        while len(registros) + invalidos < esperado:
            msg = consumer.poll(idle_timeout)
            if msg is None:
                print("Timeout sin mensajes nuevos; se detiene la lectura.")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                fila = {
                    **payload["metadata"],
                    **payload["data"],
                    "_partition": msg.partition(),
                    "_offset": msg.offset(),
                }
                registros.append(fila)
            except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as e:
                invalidos += 1
                print(f"Mensaje inválido (partición {msg.partition()}, offset {msg.offset()}): {e}")
        consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    print(f"Consumidos: {len(registros)} | Inválidos: {invalidos}")
    return pd.DataFrame(registros)


if __name__ == "__main__":
    df = consume_all()
    df.to_csv("data/raw_records.csv", index=False)
    print("\n--- Verificación ---")
    print("Shape:", df.shape)
    print("\nFilas por año:\n", df["year"].value_counts().sort_index())
    print("\nFilas por archivo:\n", df["source_file"].value_counts())
    print("\nDuplicados de record_id:", df["record_id"].duplicated().sum())
    print("\nNulos por columna:\n", df.isna().sum()[df.isna().sum() > 0])
    print("\nColumnas:\n", list(df.columns))