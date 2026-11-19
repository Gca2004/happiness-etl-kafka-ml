from confluent_kafka import Consumer, KafkaException, TopicPartition
from src.config import kafka_conf, TOPIC


def main():
    consumer = Consumer(kafka_conf())
    try:
        md = consumer.list_topics(TOPIC, timeout=15)
        topic_md = md.topics[TOPIC]
        if topic_md.error:
            raise KafkaException(topic_md.error)
        print(f"Conectado al broker. Tópico '{TOPIC}' encontrado.")
        print(f"Particiones: {len(topic_md.partitions)}")
        for pid in topic_md.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(TOPIC, pid), timeout=10
            )
            print(f"  partición {pid}: offsets {low} -> {high} ({high - low} mensajes)")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()