import os
import json
from  dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException, KafkaError


load_dotenv()

consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanism': os.getenv('SASL_MECHANISM', 'PLAIN'),
    'sasl.username': os.getenv('KAFKA_KEY'),
    'sasl.password': os.getenv('KAFKA_SECRET'),
    'group.id': 'cobros-desde-06',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['cobros-eventos'])

print("🎧 Escuchando topic cobros-eventos...\n")


import csv

eventos_procesados = [] 
    
mensajes_recibidos = 0
try:
    while True:
        msg = consumer.poll(3.0)  # Espera 1 segundo por un mensaje
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fin de la partición, no es un error crítico
                continue
            else:
                raise KafkaException(msg.error())
        
        evento = json.loads(msg.value().decode('utf-8'))
        
        eventos_procesados.append(evento)   
        
        es_cb = evento.get('motivo') == 'CB'
        es_bloqueado = evento.get('cobrador') == 'ci' and evento.get('motivo') == 'PA'
        
        estado = "BLOQUEADO" if es_bloqueado else ("OK" if es_cb else "PENDIENTE")
        
        print(f"[offset: {msg.offset()}] id_recibo: {evento.get('id_recibo')}, numsituarecib: {evento.get('numsituarecib')}, motivo: {evento.get('motivo')}, cobrador: {evento.get('cobrador')}, estado: {estado}\n")
        
        
finally:
    consumer.close()
    print(f"Total de mensajes recibidos: {mensajes_recibidos}")
      
    ruta_csv = os.path.expanduser('~/trustsync/proyectos/trustsync_dbt/seeds/eventos_procesados.csv')
    if eventos_procesados:
        with open(ruta_csv, mode='w', newline='') as file: 
            writer = csv.DictWriter(file, fieldnames=eventos_procesados[0].keys())
            writer.writeheader()
            writer.writerows(eventos_procesados)
            
        print(f"Archivo 'eventos_procesados.csv' creado con {len(eventos_procesados)} eventos procesados.")   
    else:
        print("No se procesaron eventos, no se creó el archivo CSV.")

