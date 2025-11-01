import json
import logging
import time
from kafka import KafkaConsumer, KafkaProducer
import threading
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('KafkaRouter')

class KafkaRouter:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.running = True
        self.max_retries = 10
        self.retry_delay = 10
        
    def wait_for_kafka(self):
        logger.info("Esperando a que Kafka esté disponible...")
        
        for attempt in range(self.max_retries):
            try:
                # Probar conexión creando un productor temporal
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=10000
                )
                producer.close()
                logger.info("Kafka está disponible")
                return True
            except Exception as e:
                logger.warning(f"Intento {attempt + 1}/{self.max_retries} - Kafka no disponible: {e}")
                if attempt == self.max_retries - 1:
                    logger.error("No se pudo conectar a Kafka después de todos los intentos")
                    return False
                time.sleep(self.retry_delay)
        return False

    def create_producer(self):
        for attempt in range(5):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3,
                    request_timeout_ms=15000
                )
                logger.info("Productor Kafka creado")
                return producer
            except Exception as e:
                logger.warning(f"Intento {attempt + 1}/5 - Error creando productor: {e}")
                if attempt == 4:
                    return None
                time.sleep(5)

    def route_preguntas(self):
        if not self.wait_for_kafka():
            sys.exit(1)
            
        producer = self.create_producer()
        if not producer:
            logger.error("No se pudo crear el productor")
            sys.exit(1)
            
        logger.info("Iniciando router de preguntas...")
        
        try:
            consumer = KafkaConsumer(
                'preguntas-nuevas',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='kafka-router',
                auto_offset_reset='earliest',
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            
            logger.info("Router configurado, esperando mensajes en 'preguntas-nuevas'...")
            
            for message in consumer:
                if not self.running:
                    break
                    
                data = message.value
                pregunta_id = data.get('pregunta_id', 'unknown')
                intento = data.get('intento', 0)
                
                logger.info(f"🔄 Routeando pregunta: {pregunta_id} (Intento: {intento + 1})")
                
                # Determinar el destino basado en el intento
                if intento >= 3:  # Máximo 3 reintentos
                    logger.warning(f"Pregunta excedió reintentos: {pregunta_id}")
                    producer.send('respuestas-rechazadas', {
                        **data,
                        'razon': 'max_reintentos_excedido'
                    })
                else:
                    # Enviar a preguntas-llm para procesamiento por LLM
                    producer.send('preguntas-llm', data)
                    logger.info(f"Pregunta routeada a 'preguntas-llm': {pregunta_id}")
                    
        except Exception as e:
            logger.error(f" Error en router: {e}")
            sys.exit(1)

    def start(self):
        self.route_preguntas()

if __name__ == '__main__':
    logger.info("Iniciando Kafka Router...")
    router = KafkaRouter()
    
    try:
        router.start()
    except KeyboardInterrupt:
        logger.info("Deteniendo router...")
        router.running = False
    except Exception as e:
        logger.error(f"Error fatal: {e}")

        sys.exit(1)
