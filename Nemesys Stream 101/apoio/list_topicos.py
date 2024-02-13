import os
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP = [
    "127.0.0.1:9094"
]

def topics():
    cluster = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)

    # brokers = cluster.brokers()
    topicos = cluster.topics()

    lista_topicos = []
    for topico in topicos:
        lista_topicos.append(str(topico)) 

    lista_topicos.sort()
    return lista_topicos

def main():
    print("list_topics")
    topicos = topics()
    print(f"Encontrado {len(topicos)} topicos")

    for topico in topicos:
        print(topico)


if __name__ == "__main__":
    main()
