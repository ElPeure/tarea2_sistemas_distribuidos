import json
import logging
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from flask import Flask, jsonify
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self):
        self.metrics = {
            'throughput': defaultdict(list),
            'queue_times': defaultdict(list),
            'message_counts': defaultdict(int),
            'start_time': time.time(),
            'first_message_time': None,
            'last_message_time': None
        }
        self.message_timestamps = defaultdict(dict)
        
    def record_message_sent(self, topic, message_id):
        self.message_timestamps[message_id] = {
            'sent_time': time.time(),
            'topic': topic
        }
        self.metrics['message_counts'][topic] += 1
        
        if not self.metrics['first_message_time']:
            self.metrics['first_message_time'] = time.time()
            
    def record_message_processed(self, message_id, destination_topic=None):
        if message_id in self.message_timestamps:
            sent_time = self.message_timestamps[message_id]['sent_time']
            processing_time = time.time() - sent_time
            
            original_topic = self.message_timestamps[message_id]['topic']
            self.metrics['queue_times'][original_topic].append(processing_time)
            
            if destination_topic:
                self.metrics['throughput'][destination_topic].append(time.time())
                
            self.metrics['last_message_time'] = time.time()
            
    def get_summary(self):
        total_time = time.time() - self.metrics['start_time']
        active_time = (self.metrics['last_message_time'] or time.time()) - (self.metrics['first_message_time'] or time.time())
        
        throughput_summary = {}
        for topic, timestamps in self.metrics['throughput'].items():
            if timestamps:
                time_range = max(timestamps) - min(timestamps) if len(timestamps) > 1 else active_time
                throughput_summary[topic] = len(timestamps) / (time_range / 60)
        
        queue_times_summary = {}
        for topic, times in self.metrics['queue_times'].items():
            if times:
                queue_times_summary[topic] = {
                    'avg': np.mean(times),
                    'min': np.min(times),
                    'max': np.max(times),
                    'count': len(times)
                }
        
        return {
            'total_messages': sum(self.metrics['message_counts'].values()),
            'messages_by_topic': dict(self.metrics['message_counts']),
            'throughput_per_minute': throughput_summary,
            'queue_times_seconds': queue_times_summary,
            'total_processing_time_seconds': active_time,
            'system_uptime_seconds': total_time
        }

class KafkaManager:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topics = [
            'preguntas-nuevas',
            'preguntas-llm', 
            'respuestas-llm',
            'respuestas-validadas',
            'respuestas-rechazadas'
        ]
        self.metrics = MetricsCollector()
        self.setup_topics()
        self.producer = self.create_producer()
        self.start_metrics_consumer()
        
    def setup_topics(self):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='kafka_manager'
            )
            
            existing_topics = admin_client.list_topics()
            topics_to_create = []
            
            for topic in self.topics:
                if topic not in existing_topics:
                    topics_to_create.append(
                        NewTopic(
                            name=topic,
                            num_partitions=3,
                            replication_factor=1
                        )
                    )
                    logger.info(f"Topic creado: {topic}")
            
            if topics_to_create:
                admin_client.create_topics(new_topics=topics_to_create)
                logger.info("Todos los topics de Kafka configurados")
            else:
                logger.info("Topics de Kafka ya existen")
                
            admin_client.close()
                
        except Exception as e:
            logger.error(f"Error configurando topics: {e}")

    def create_producer(self):
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Productor Kafka creado")
            return producer
        except Exception as e:
            logger.error(f"Error creando productor: {e}")
            return None

    def start_metrics_consumer(self):
        def consume_metrics():
            try:
                consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id='kafka_metrics_collector',
                    auto_offset_reset='earliest'
                )
                
                logger.info("Iniciando recolector de métricas...")
                
                for message in consumer:
                    data = message.value
                    message_id = data.get('pregunta_id', 'unknown')
                    
                    if message.topic == 'preguntas-nuevas':
                        self.metrics.record_message_sent(message.topic, message_id)
                    else:
                        self.metrics.record_message_processed(message_id, message.topic)
                        
            except Exception as e:
                logger.error(f"Error en recolector de métricas: {e}")
                
        thread = threading.Thread(target=consume_metrics, daemon=True)
        thread.start()

    def send_message(self, topic, message, key=None):
        if self.producer:
            try:
                message_id = message.get('pregunta_id', 'unknown')
                self.metrics.record_message_sent(topic, message_id)
                
                future = self.producer.send(topic, value=message, key=key)
                self.producer.flush()
                logger.info(f"Mensaje enviado a {topic}: {message_id}")
                return True
            except Exception as e:
                logger.error(f"Error enviando mensaje: {e}")
                return False
        return False

    def print_dashboard(self):
        summary = self.metrics.get_summary()
        
        print("\n" + "="*80)
        print("DASHBOARD KAFKA - MÉTRICAS DEL SISTEMA")
        print("="*80)
        
        print(f"\nTHROUGHPUT GENERAL")
        print("-" * 40)
        print(f"Total mensajes procesados: {summary['total_messages']}")
        print(f"Tiempo total procesamiento: {summary['total_processing_time_seconds']:.2f}s")
        print(f"Uptime del sistema: {summary['system_uptime_seconds']:.2f}s")
        
        print(f"\nTHROUGHPUT POR TOPIC (mensajes/minuto)")
        print("-" * 50)
        for topic, throughput in summary['throughput_per_minute'].items():
            print(f"  {topic:<25}: {throughput:>6.2f} msg/min")
        
        print(f"\nTIEMPOS EN COLA POR TOPIC")
        print("-" * 50)
        for topic, times in summary['queue_times_seconds'].items():
            print(f"  {topic:<25}:")
            print(f"    • Promedio: {times['avg']:.2f}s")
            print(f"    • Mínimo:   {times['min']:.2f}s") 
            print(f"    • Máximo:   {times['max']:.2f}s")
            print(f"    • Total:    {times['count']} mensajes")
        
        print(f"\nCONTEO DE MENSAJES POR TOPIC")
        print("-" * 40)
        for topic, count in summary['messages_by_topic'].items():
            print(f"  {topic:<25}: {count:>6} mensajes")
            
        print("\n" + "="*80)

app = Flask(__name__)
kafka_manager = KafkaManager()

@app.route('/health', methods=['GET'])
def health():
    try:
        producer = kafka_manager.create_producer()
        if producer:
            producer.close()
            return jsonify({
                "status": "healthy", 
                "service": "kafka_manager",
                "topics": kafka_manager.topics
            })
        else:
            return jsonify({"status": "unhealthy"}), 500
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/metrics', methods=['GET'])
def get_metrics():
    return jsonify(kafka_manager.metrics.get_summary())

@app.route('/dashboard', methods=['GET'])
def show_dashboard():
    kafka_manager.print_dashboard()
    return jsonify({"message": "Dashboard impreso en consola"})

@app.route('/send_pregunta', methods=['POST'])
def send_pregunta():
    from flask import request
    data = request.get_json()
    
    if not data or 'pregunta' not in data or 'respuesta_real' not in data:
        return jsonify({"error": "Datos incompletos"}), 400
    
    message = {
        "pregunta_id": f"preg_{int(time.time()*1000)}",
        "pregunta": data['pregunta'],
        "respuesta_real": data['respuesta_real'],
        "timestamp": time.time(),
        "source": "main_app"
    }
    
    success = kafka_manager.send_message('preguntas-nuevas', message)
    
    return jsonify({
        "success": success,
        "pregunta_id": message['pregunta_id'],
        "topic": "preguntas-nuevas"
    })

@app.route('/topics', methods=['GET'])
def get_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_manager.bootstrap_servers,
            client_id='topic_lister'
        )
        topics = admin_client.list_topics()
        admin_client.close()
        return jsonify({"topics": list(topics)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("Kafka Manager iniciado")   
    
    app.run(host='0.0.0.0', port=8004, debug=False)