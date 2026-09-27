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

COMENTARIOS_CON_PII = [
    "El asegurado Juan García Martínez con DNI 12345678A notificó el siniestro por teléfono 612345678",
    "Titular María López Ruiz, NIF 87654321B, domicilio en Calle Mayor 15, Madrid. Contacto: maria.lopez@email.com",
    "Peritaje realizado en domicilio del asegurado Carlos Fernández, DNI 11223344C, Avenida Diagonal 200, Barcelona",
    "Cobrador ag1 contactó con Ana Martínez García (DNI 55667788D) para gestionar el impago del recibo",
    "Siniestro declarado por Pedro Sánchez López, teléfono 698765432, email pedro.sanchez@gmail.com",
    "Asegurado Roberto González, NIF 99887766E, residente en Paseo de la Castellana 45, Madrid 28046",
    "Expediente abierto para Laura Jiménez Pérez DNI 44332211F tras accidente de tráfico en A-6 km 23",
    "Notificación enviada a Isabel Romero (NIF 22334455G) en Calle Serrano 78, Madrid. Tlf: 911234567",
]

COMENTARIOS_SIN_PII = [
    "Siniestro procesado correctamente según protocolo interno",
    "Documentación completa recibida y validada por el departamento",
    "Recibo pendiente de revisión por el equipo de cobros",
    "Movimiento registrado automáticamente por el sistema",
    "Incidencia resuelta sin necesidad de intervención manual",
]

def generar_comentario():
    # 65% de probabilidad de comentario con PII
    if random.random() < 0.65:
        return random.choice(COMENTARIOS_CON_PII)
    return random.choice(COMENTARIOS_SIN_PII)


def generar_evento_cobro() -> dict:
    return {
        "id_recibo": f"REC-{random.randint(1, 20):04d}",
        "numsituarecib": random.randint(1, 20),
        "motivo": random.choice(MOTIVOS),
        "cobrador": random.choice(COBRADORES),
        "comentario": generar_comentario(),
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
            