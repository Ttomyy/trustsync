import os
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from  dotenv import load_dotenv
load_dotenv()
# ── Kafka config ──────────────────────────────────────────
consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
        'sasl.mechanism': os.getenv('SASL_MECHANISM', 'PLAIN'),
        'sasl.username': os.getenv('KAFKA_KEY'),
        'sasl.password': os.getenv('KAFKA_SECRET'),
        'group.id': 'trustsync-mongo-02',
        'auto.offset.reset': 'earliest'
})

# ── MongoDB Atlas config ───────────────────────────────────
MONGO_URI = (
    #f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASS')}"
    #f"@{os.getenv('MONGO_CLUSTER')}/?retryWrites=true&w=majority"
    os.getenv('MONGO_URI')
)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[os.getenv('MONGO_DB')]
coleccion = db['cobros_eventos']

print("🎧 Escuchando topic cobros-eventos → MongoDB Atlas...\n")

eventos_insertados = 0
consumer.subscribe(['cobros-eventos'])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Error Kafka: {msg.error()}")
            break

        evento = json.loads(msg.value().decode('utf-8'))

        # Enriquecer el evento antes de guardarlo en Mongo
        evento['_kafka_offset'] = msg.offset()
        evento['_kafka_partition'] = msg.partition()
        evento['_ingested_at'] = datetime.now(timezone.utc).isoformat()

        # Clasificación TrustSync
        es_bloqueado = (evento.get('cobrador') == 'ci'
                        and evento.get('motivo') == 'PA')
        es_cb = evento.get('motivo') == 'CB'
        evento['estado_evento'] = (
            'BLOQUEADO' if es_bloqueado else
            'COBRO'     if es_cb else
            'MOVIMIENTO'
        )

        # Insertar en MongoDB Atlas
        coleccion.insert_one(evento)
        eventos_insertados += 1

        print(f"[offset {msg.offset()}] "
              f"{evento['estado_evento']:10} | "
              f"{evento.get('id_recibo')} | "
              f"{evento.get('motivo')} | "
              f"{evento.get('cobrador')}")

except KeyboardInterrupt:
    print(f"\n⛔ Parado. Eventos insertados en Atlas: {eventos_insertados}")

finally:
    consumer.close()
    mongo_client.close()
    print("✅ Conexiones cerradas.")