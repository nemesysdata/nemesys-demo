import os
import socket
import ssl
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP = [
    # "127.0.0.1:9094"
    "nemesys-stream-101-kafka-bootstrap.nemesys-stream-101.svc:9093"
]

def checkKafka(host, port):
    print("Connecting to host")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host, port))
            print("Connected")
        except Exception as e:
            print(e)
        
def topics():
    ssl_context = ssl._create_unverified_context()


    cluster = KafkaConsumer(security_protocol="SSL", ssl_context=ssl_context, bootstrap_servers=KAFKA_BOOTSTRAP)

    # brokers = cluster.brokers()
    topicos = cluster.topics()

    lista_topicos = []
    for topico in topicos:
        lista_topicos.append(str(topico)) 

    lista_topicos.sort()
    return lista_topicos

def main():
    checkKafka("nemesys-stream-101-kafka-bootstrap.nemesys-stream-101.svc", 9093)
    print("list_topics")
    topicos = topics()
    print(f"Encontrado {len(topicos)} topicos")

    for topico in topicos:
        print(topico)

if __name__ == "__main__":
    main()
