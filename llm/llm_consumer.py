import json
import logging
import time
import requests
from kafka import KafkaConsumer, KafkaProducer
import threading
from flask import Flask, jsonify
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('LLMService')

class LLMConsumer:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.ollama_url = self.find_ollama_url()
        self.producer = None
        self.running = True
        self.setup_producer()
        logger.info("🚀 LLM Service inicializado")
        
    def find_ollama_url(self):
        possible_urls = [
            "http://ollama:11434",
            "http://localhost:11434"
        ]
        
        for url in possible_urls:
            try:
                logger.info(f"🔍 Probando conexión con Ollama en: {url}")
                response = requests.get(f"{url}/api/tags", timeout=10)
                if response.status_code == 200:
                    logger.info(f"Ollama encontrado en: {url}")
                    return url
            except Exception as e:
                logger.warning(f" {url} no disponible: {e}")
        
        logger.error("No se pudo encontrar Ollama")
        return None

    def setup_producer(self):
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=10,
                    request_timeout_ms=30000,
                    linger_ms=100,
                    batch_size=16384
                )
                # Test de conexión
                self.producer.flush(timeout=10)
                logger.info("Productor Kafka creado y conectado")
                return
            except Exception as e:
                logger.error(f"Intento {attempt + 1}/{max_retries} - Error creando productor: {e}")
                if attempt == max_retries - 1:
                    logger.error("No se pudo crear el productor después de todos los intentos")
                    self.producer = None
                time.sleep(5)

    def get_llm_response(self, pregunta):
        if self.ollama_url is None:
            return "Error: Ollama no disponible"
        
        try:
            url = f"{self.ollama_url}/api/generate"
            logger.info(f"Consultando LLM phi3: {pregunta[:100]}...")
            
            payload = {
                "model": "phi3",
                "stream": False,
                "prompt": pregunta,
                "options": {
                    "num_predict": 100,
                    "temperature": 0.7,
                    "top_k": 40,
                    "top_p": 0.9
                }
            }
            
            response = requests.post(
                url, 
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                data = response.json()
                respuesta = data.get('response', 'No response field')
                logger.info(f"Respuesta phi3 recibida: {respuesta[:150]}...")
                return respuesta.strip()
            else:
                error_msg = f"Error HTTP {response.status_code}: {response.text}"
                logger.error(f"{error_msg}")
                return f"Error: {error_msg}"
                
        except requests.exceptions.Timeout:
            logger.error("Timeout con Ollama (60s)")
            return "Error: Timeout excedido"
        except Exception as e:
            logger.error(f"Error inesperado con Ollama: {e}")
            return f"Error: {str(e)}"

    def send_to_kafka(self, topic, data):
        if not self.producer:
            logger.error("Productor Kafka no disponible")
            return False
            
        try:
            pregunta_id = data.get('pregunta_id', 'unknown')
            key = pregunta_id
            
            logger.info(f"Intentando enviar a {topic}: {pregunta_id}")
            
            future = self.producer.send(topic, value=data, key=key)
            result = future.get(timeout=10)
            logger.info(f"MENSAJE ENVIADO EXITOSAMENTE a {topic}: {pregunta_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando a Kafka {topic}: {e}")
            return False

    def process_preguntas(self):
        logger.info("Iniciando consumo de preguntas desde 'preguntas-llm'...")
        
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries and self.running:
            try:
                consumer = KafkaConsumer(
                    'preguntas-llm',
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id='llm-consumer-group',
                    auto_offset_reset='earliest',
                    session_timeout_ms=60000,
                    heartbeat_interval_ms=20000,
                    max_poll_interval_ms=300000
                )
                
                logger.info("Esperando mensajes de preguntas...")
                
                for message in consumer:
                    if not self.running:
                        break
                        
                    data = message.value
                    pregunta_id = data.get('pregunta_id', 'unknown')
                    
                    logger.info(f"LLM RECIBIÓ PREGUNTA - ID: {pregunta_id}")
                    logger.info(f"Procesando pregunta: {data.get('pregunta', '')[:100]}...")
                    
                    respuesta_llm = self.get_llm_response(data.get('pregunta', ''))
                    
                    respuesta_data = {
                        **data,
                        'respuesta_llm': respuesta_llm,
                        'procesado_por': 'llm_service',
                        'timestamp_llm': time.time(),
                        'ollama_url': self.ollama_url
                    }
                    
                    success = self.send_to_kafka('respuestas-llm', respuesta_data)
                    
                    if success:
                        logger.info(f"RESPUESTA ENVIADA A FLINK - ID: {pregunta_id}")
                    else:
                        logger.error(f"FALLO ENVÍO A FLINK - ID: {pregunta_id}")
                    
                    consumer.commit()
                    
                consumer.close()
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Reconectando consumidor... ({retry_count}/{max_retries})")
                    time.sleep(10)
                    
            except Exception as e:
                logger.error(f"Error en LLM consumer: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Reintentando en 10 segundos... ({retry_count}/{max_retries})")
                    time.sleep(10)
                else:
                    logger.error("Máximos reintentos alcanzados en LLM Consumer")

    def start_processing(self):
        self.processing_thread = threading.Thread(target=self.process_preguntas)
        self.processing_thread.daemon = True
        self.processing_thread.start()
        logger.info("LLM Consumer iniciado")

app = Flask(__name__)
llm_consumer = LLMConsumer()

@app.route('/health', methods=['GET'])
def health():
    ollama_status = "disconnected"
    kafka_status = "disconnected"
    
    if llm_consumer.ollama_url:
        try:
            response = requests.get(f"{llm_consumer.ollama_url}/api/tags", timeout=5)
            ollama_status = "connected" if response.status_code == 200 else "error"
        except:
            ollama_status = "error"
    
    if llm_consumer.producer:
        try:
            llm_consumer.producer.flush(timeout=5)
            kafka_status = "connected"
        except:
            kafka_status = "error"
    
    return jsonify({
        "status": "healthy", 
        "service": "llm_consumer",
        "ollama": ollama_status,
        "kafka": kafka_status,
        "processing": llm_consumer.running
    })

@app.route('/test_kafka', methods=['POST'])
def test_kafka():
    test_data = {
        "pregunta_id": f"test_{int(time.time())}",
        "pregunta": "Test question",
        "respuesta_real": "Test answer", 
        "respuesta_llm": "Test LLM response",
        "timestamp": time.time()
    }
    
    success = llm_consumer.send_to_kafka('respuestas-llm', test_data)
    
    return jsonify({
        "success": success,
        "message": "Test message sent to respuestas-llm" if success else "Failed to send test message"
    })

if __name__ == '__main__':
    print("Consumiendo preguntas de Kafka...")
    
    time.sleep(15)
    llm_consumer.start_processing()
    app.run(host='0.0.0.0', port=8003, debug=False, use_reloader=False)