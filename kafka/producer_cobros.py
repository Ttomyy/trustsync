import os
from dotenv import load_dotenv

# Carga las variables del archivo .env que está en la misma carpeta
load_dotenv()
import json
import random
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException

def get_kafka_config() -> dict:
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    key = os.getenv('KAFKA_KEY')   
    secret = os.getenv('KAFKA_SECRET')
    
    if not all([bootstrap_servers, key, secret]):
        raise ValueError("Missing required Kafka nvironment variables.")
    """
    Retrieves Kafka configuration from environment variables.
    Returns a dictionary with the necessary configuration for the Kafka producer.
    """
    
    return {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'client.id': os.getenv('KAFKA_CLIENT_ID', 'producer_cobros'),
        'sasl.mechanism': os.getenv('SASL_MECHANISM', 'PLAIN'),
        'sasl.username': key,
        'sasl.password': secret,
        'acks': 'all'
    }
    
def delivery_report(err, msg):
    """
    Callback function to report the delivery status of a message.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message Enviado a {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        
MOTIVOS = ['DB','CB','PA','PB']
COBRADORES = ['ag1','ag2','ag3','ci']

def generar_evento_cobro() -> dict:
    return {
        "id_recibo": f"REC-{random.randint(1, 20):04d}",
        "numsituarecib": random.randint(1, 20),
        "motivo": random.choice(MOTIVOS),
        "cobrador": random.choice(COBRADORES),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
def run_producer(topic: str = 'cobros-eventos', total_messages: int = 100):
    config = get_kafka_config()
    producer = Producer(config)
    print(f"Starting Kafka producer for topic '{topic}' with {total_messages} messages.")
    
    try:
        for _ in range(total_messages):
            evento_cobro = generar_evento_cobro()
            producer.produce(
                topic=topic, 
                key=evento_cobro['id_recibo'], 
                value=json.dumps(evento_cobro), 
                callback=delivery_report
                )
    except BufferError:
        print("buffer local lleno, esperando a que se libere espacio...")
        
    except KafkaException as e:
        print(f"Error en la producción de mensajes: {e}")
    finally:
        print("Estperando vaciado del buffer antes de cerrar el productor...")
        producer.flush(timeout=10)
        print("Todos los mensajes han sido enviados.")
        
        
if __name__ == "__main__":
    run_producer()
            