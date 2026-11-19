import json
from confluent_kafka import Consumer, KafkaError
from src.config import kafka_conf, TOPIC

N_SAMPLE = 10  # cuántos mensajes mostrar


def main():
    consumer = Consumer(kafka_conf())
    consumer.subscribe([TOPIC])
    shown = 0
    try:
        while shown < N_SAMPLE:
            # El primer poll puede tardar unos segundos (unión al grupo)
            msg = consumer.poll(15.0)
            if msg is None:
                print("Sin mensajes (timeout).")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Fin de la partición.")
                    break
                print("Error:", msg.error())
                break

            shown += 1
            ts_type, ts_ms = msg.timestamp()
            print(f"\n=== Mensaje {shown} ===")
            print(f"topic={msg.topic()} partition={msg.partition()} "
                  f"offset={msg.offset()} timestamp={ts_ms}")
            print(f"key={msg.key()}")
            print(f"headers={msg.headers()}")

            raw = msg.value().decode("utf-8")
            try:
                data = json.loads(raw)
                print("value (JSON):")
                print(json.dumps(data, indent=2, ensure_ascii=False))
            except json.JSONDecodeError:
                print("value (texto crudo):", raw)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()