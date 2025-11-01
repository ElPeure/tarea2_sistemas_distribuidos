import json
import logging
import time
from kafka import KafkaConsumer, KafkaProducer
import threading
from flask import Flask, jsonify, request
import numpy as np
import sys
import os
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class FlinkMetrics:
    def __init__(self, metrics_file='/app/data/flink_metrics.json'):
        self.metrics_file = metrics_file
        self.scores = []
        self.decisions = defaultdict(int)
        self.feedback_loop_data = {
            'reintentos': defaultdict(int),
            'score_evolution': [],
            'effectiveness_by_intento': defaultdict(list)
        }
        self.start_time = time.time()
        self.load_persistent_data()
        
    def load_persistent_data(self):
        try:
            os.makedirs('/app/data', exist_ok=True)
            if os.path.exists(self.metrics_file):
                with open(self.metrics_file, 'r') as f:
                    data = json.load(f)
                    self.scores = data.get('scores', [])
                    self.decisions = defaultdict(int, data.get('decisions', {}))
                    self.feedback_loop_data = data.get('feedback_loop_data', {
                        'reintentos': defaultdict(int),
                        'score_evolution': [],
                        'effectiveness_by_intento': defaultdict(list)
                    })
                logger.info(f"Métricas persistentes cargadas: {len(self.scores)} scores")
            else:
                logger.info("Iniciando métricas nuevas")
        except Exception as e:
            logger.error(f"Error cargando métricas persistentes: {e}")
    
    def save_persistent_data(self):
        try:
            metrics_data = {
                'scores': self.scores,
                'decisions': dict(self.decisions),
                'feedback_loop_data': {
                    'reintentos': dict(self.feedback_loop_data['reintentos']),
                    'score_evolution': self.feedback_loop_data['score_evolution'],
                    'effectiveness_by_intento': {
                        k: v for k, v in self.feedback_loop_data['effectiveness_by_intento'].items()
                    }
                },
                'last_updated': time.time()
            }
            with open(self.metrics_file, 'w') as f:
                json.dump(metrics_data, f)
        except Exception as e:
            logger.error(f"Error guardando métricas persistentes: {e}")
        
    def record_processing(self, data, score, decision):
        self.scores.append(score)
        self.decisions[decision] += 1
        
        intento = data.get('intento', 0)
        self.feedback_loop_data['reintentos'][intento] += 1
        self.feedback_loop_data['score_evolution'].append({
            'timestamp': time.time(),
            'score': score,
            'intento': intento,
            'decision': decision
        })
        self.feedback_loop_data['effectiveness_by_intento'][intento].append(score)
        
        # Guardar después de cada procesamiento
        self.save_persistent_data()
        
    def get_summary(self, threshold):
        if not self.scores:
            return {}
            
        scores_array = np.array(self.scores)
        
        return {
            'score_threshold': threshold,
            'total_processed': len(self.scores),
            'score_stats': {
                'mean': float(np.mean(scores_array)),
                'std': float(np.std(scores_array)),
                'min': float(np.min(scores_array)),
                'max': float(np.max(scores_array)),
                'median': float(np.median(scores_array))
            },
            'decisions': dict(self.decisions),
            'feedback_loop_analysis': {
                'reintentos_distribution': dict(self.feedback_loop_data['reintentos']),
                'effectiveness_by_intento': {
                    intento: {
                        'mean': float(np.mean(scores)),
                        'count': len(scores)
                    }
                    for intento, scores in self.feedback_loop_data['effectiveness_by_intento'].items()
                },
                'improvement_rate': self.calculate_improvement_rate()
            },
            'processing_time': time.time() - self.start_time
        }
    
    def calculate_improvement_rate(self):
        improvement_data = {}
        for intento, scores in self.feedback_loop_data['effectiveness_by_intento'].items():
            if len(scores) > 0:
                improvement_data[intento] = {
                    'mean_score': float(np.mean(scores)),
                    'improvement_from_previous': None
                }
        
        intentos_ordenados = sorted(improvement_data.keys())
        for i in range(1, len(intentos_ordenados)):
            current = intentos_ordenados[i]
            previous = intentos_ordenados[i-1]
            current_mean = improvement_data[current]['mean_score']
            previous_mean = improvement_data[previous]['mean_score']
            
            if previous_mean > 0:
                improvement = ((current_mean - previous_mean) / previous_mean) * 100
                improvement_data[current]['improvement_from_previous'] = improvement
                
        return improvement_data

class FlinkProcessor:
    def __init__(self, bootstrap_servers='kafka:9092', score_threshold=0.7):
        self.bootstrap_servers = bootstrap_servers
        self.score_threshold = float(score_threshold)
        self.model = None
        self.producer = self.create_producer()
        self.running = True
        self.metrics = FlinkMetrics()
        self.load_model_async()
        
    def load_model_async(self):
        def load_model():
            try:
                logger.info("Cargando modelo SentenceTransformer...")
                from sentence_transformers import SentenceTransformer, util
                
                logger.info("Módulos SentenceTransformer importados correctamente")
                
                self.model = SentenceTransformer("all-MiniLM-L6-v2")
                logger.info("Modelo cargado exitosamente")
                
                test_text = "Hello world"
                embedding = self.model.encode(test_text)
                logger.info(f"Test embedding generado - dim: {embedding.shape}")
                
            except ImportError as e:
                logger.error(f"No se pueden importar módulos: {e}")
                self.model = None
            except Exception as e:
                logger.error(f"Error cargando modelo: {e}")
                self.model = None
        
        thread = threading.Thread(target=load_model)
        thread.daemon = True
        thread.start()

    def create_producer(self):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3,
                    request_timeout_ms=30000,
                    acks='all'
                )
                logger.info("Productor Kafka creado en Flink")
                return producer
            except Exception as e:
                logger.warning(f"Intento {attempt + 1}/{max_retries} - Error creando productor: {e}")
                if attempt == max_retries - 1:
                    logger.error("No se pudo crear el productor después de todos los intentos")
                    return None
                time.sleep(5)

    def calculate_similarity_simple(self, respuesta_llm, respuesta_real):
        try:
            if not respuesta_llm or not respuesta_real:
                logger.warning(" Una de las respuestas está vacía en cálculo simple")
                return 0.0
                
            # Normalizar texto
            resp_llm = respuesta_llm.lower().strip()
            resp_real = respuesta_real.lower().strip()
            
            if resp_llm == resp_real:
                return 1.0
                
            words_llm = set(resp_llm.split())
            words_real = set(resp_real.split())
            
            if not words_llm or not words_real:
                return 0.0
            
            intersection = len(words_llm.intersection(words_real))
            union = len(words_llm.union(words_real))
            jaccard = intersection / union if union else 0.0
            
            # Containment (qué porcentaje de palabras de la respuesta real está en la llm)
            containment = intersection / len(words_real) if words_real else 0.0
            
            overlap = intersection / min(len(words_llm), len(words_real)) if min(len(words_llm), len(words_real)) > 0 else 0.0
            
            final_score = (jaccard + containment + overlap) / 3.0
            
            logger.info(f"Score simple - Jaccard: {jaccard:.3f}, Containment: {containment:.3f}, Overlap: {overlap:.3f} -> Final: {final_score:.3f}")
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error en cálculo simple: {e}")
            return 0.0

    def calculate_similarity_advanced(self, respuesta_llm, respuesta_real):
        if not respuesta_llm or not respuesta_real:
            logger.warning("Una de las respuestas está vacía")
            return 0.0
            
        if isinstance(respuesta_llm, str) and respuesta_llm.startswith("Error:"):
            logger.warning("Respuesta LLM contiene error")
            return 0.1
        
        if self.model is None:
            logger.warning("Modelo no disponible, usando cálculo simple")
            return self.calculate_similarity_simple(respuesta_llm, respuesta_real)
        
        try:
            logger.info(f"Calculando embeddings...")
            
            emb1 = self.model.encode(respuesta_llm, convert_to_tensor=True)
            emb2 = self.model.encode(respuesta_real, convert_to_tensor=True)
            
            logger.info(f"Embeddings calculados - dims: {emb1.shape}, {emb2.shape}")
            
            from sentence_transformers import util
            score = util.cos_sim(emb1, emb2).item()
            
            logger.info(f"Score avanzado: {score:.4f}")
            return score
            
        except Exception as e:
            logger.error(f"Error en cálculo avanzado: {e}")
            logger.info("Fallback a cálculo simple...")
            return self.calculate_similarity_simple(respuesta_llm, respuesta_real)

    def diagnose_kafka_topics(self):
        try:
            from kafka import KafkaConsumer
            
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
            topics = consumer.topics()
            logger.info(f"Topics disponibles: {topics}")
            
            # Ver mensajes en respuestas-llm
            temp_consumer = KafkaConsumer(
                'respuestas-llm',
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=5000  # Timeout después de 5 segundos
            )
            
            message_count = 0
            for message in temp_consumer:
                message_count += 1
                if message_count <= 3:  # Mostrar primeros 3 mensajes
                    logger.info(f"Mensaje #{message_count} en respuestas-llm: {message.value}")
            
            logger.info(f"Total mensajes en respuestas-llm: {message_count}")
            temp_consumer.close()
            
        except Exception as e:
            logger.error(f"Error en diagnóstico Kafka: {e}")

    def process_respuestas_llm(self):
        logger.info("Iniciando procesamiento de respuestas LLM desde 'respuestas-llm'...")
        
        logger.info("Ejecutando diagnóstico de Kafka...")
        self.diagnose_kafka_topics()
        
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries and self.running:
            try:
                consumer = KafkaConsumer(
                    'respuestas-llm',
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id='flink-processor-group-v2',  
                    auto_offset_reset='earliest',
                    session_timeout_ms=60000,
                    heartbeat_interval_ms=20000,
                    max_poll_interval_ms=300000,
                    enable_auto_commit=False  
                )
                
                subscription = consumer.subscription()
                logger.info(f"📡 Flink suscrito a: {subscription}")
                
                for topic_partition in consumer.assignment():
                    beginning = consumer.beginning_offsets([topic_partition])[topic_partition]
                    end = consumer.end_offsets([topic_partition])[topic_partition]
                    current = consumer.position(topic_partition)
                    logger.info(f" Topic {topic_partition}: offsets inicio={beginning}, fin={end}, actual={current}")
                
                logger.info(" Esperando mensajes de respuestas LLM...")
                
                message_count = 0
                for message in consumer:
                    if not self.running:
                        break
                        
                    message_count += 1
                    data = message.value
                    pregunta_id = data.get('pregunta_id', 'unknown')
                    
                    logger.info(f"FLINK RECIBIÓ MENSAJE #{message_count}")
                    logger.info(f"Datos recibidos - ID: {pregunta_id}")
                    logger.info(f"Pregunta: {data.get('pregunta', '')[:100]}...")
                    logger.info(f"Respuesta LLM: {data.get('respuesta_llm', '')[:100]}...")
                    logger.info(f"Respuesta real: {data.get('respuesta_real', '')[:100]}...")
                    logger.info(f"Intento actual: {data.get('intento', 0)}")
                    
                    try:
                        respuesta_llm = data.get('respuesta_llm', '')
                        respuesta_real = data.get('respuesta_real', '')
                        
                        logger.info(f" Calculando similitud...")
                        
                        score = self.calculate_similarity_advanced(respuesta_llm, respuesta_real)
                        
                        logger.info(f"SCORE CALCULADO: {score:.4f} para ID: {pregunta_id}")
                        
                        if score == 0.0:
                            logger.warning(f"SCORE CERO - Posible error en cálculo de similitud")
                            score_simple = self.calculate_similarity_simple(respuesta_llm, respuesta_real)
                            logger.info(f"Score simple: {score_simple:.4f}")
                            if score_simple > 0:
                                score = score_simple
                                logger.info(f"Usando score simple: {score:.4f}")
                        
                    except Exception as e:
                        logger.error(f" ERROR calculando score: {e}")
                        score = 0.0
                    
                    processed_data = {
                        **data,
                        'score': score,
                        'score_calculado': score,
                        'procesado_por': 'flink',
                        'timestamp_procesado': time.time()
                    }
                    
                    decision = None
                    
                    if score >= self.score_threshold:
                        decision = 'validated'
                        if self.producer:
                            self.producer.send('respuestas-validadas', processed_data)
                            self.producer.flush()
                            logger.info(f"FLINK VALIDÓ - Score: {score:.4f} - ID: {pregunta_id}")
                            
                    else:
                        intento_actual = data.get('intento', 0)
                        
                        if intento_actual < 2:
                            decision = 'retry'
                            reintento_data = {
                                'pregunta_id': pregunta_id,
                                'pregunta': data.get('pregunta'),
                                'respuesta_real': data.get('respuesta_real'),
                                'intento': intento_actual + 1,
                                'score_anterior': score,
                                'timestamp_reintento': time.time(),
                                'razon_reintento': f'score_bajo_{score:.4f}'
                            }
                            
                            if self.producer:
                                self.producer.send('preguntas-nuevas', reintento_data)
                                self.producer.flush()
                                logger.info(f"FLINK REINTENTA - Score: {score:.4f} - Intento: {intento_actual + 1}/3 - ID: {pregunta_id}")
                                
                        else:
                            decision = 'rejected'
                            if self.producer:
                                self.producer.send('respuestas-rechazadas', {
                                    **processed_data,
                                    'razon_rechazo': 'max_reintentos_score_bajo'
                                })
                                self.producer.flush()
                                logger.warning(f"FLINK RECHAZÓ - Score: {score:.4f} - Máximos reintentos - ID: {pregunta_id}")
                    
                    if decision:
                        self.metrics.record_processing(data, score, decision)
                        logger.info(f"Métricas actualizadas - Decisión: {decision}, Score: {score:.4f}")
                    
                    consumer.commit()
                    logger.info(f"FLINK COMMIT OFFSET para mensaje #{message_count}")
                    
                consumer.close()
                logger.info(f"Procesamiento completado - Total mensajes: {message_count}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Reconectando consumidor Flink... ({retry_count}/{max_retries})")
                    time.sleep(10)
                    
            except Exception as e:
                logger.error(f"ERROR en Flink Processor: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Reintentando Flink en 10 segundos... ({retry_count}/{max_retries})")
                    time.sleep(10)
                else:
                    logger.error("Máximos reintentos alcanzados en Flink Processor")

    def start_processing(self):
        logger.info("Esperando a que el modelo se cargue...")
        for i in range(60):
            if self.model is not None:
                logger.info("Modelo cargado, iniciando procesamiento...")
                break
            if i % 10 == 0:
                logger.info(f" Esperando modelo... ({i}s)")
            time.sleep(1)
        
        if self.model is None:
            logger.warning("Modelo no cargado después de 60s, continuando con cálculo simple")
        
        self.processing_thread = threading.Thread(target=self.process_respuestas_llm)
        self.processing_thread.daemon = True
        self.processing_thread.start()
        logger.info("Flink Processor iniciado")

    def stop_processing(self):
        self.running = False
        logger.info("Flink Processor detenido")

    def print_analytics(self):
        summary = self.metrics.get_summary(self.score_threshold)
        
        if not summary:
            print("No hay datos suficientes para analytics")
            return
            
        print("\n" + "="*80)
        print("ANALYTICS FLINK - FEEDBACK LOOP EFFECTIVENESS")
        print("="*80)
        
        print(f"\nCONFIGURACIÓN")
        print("-" * 30)
        print(f"Umbral de score: {summary['score_threshold']}")
        print(f"Total procesado: {summary['total_processed']}")
        print(f"Tiempo procesamiento: {summary['processing_time']:.2f}s")
        
        print(f"\nESTADÍSTICAS DE SCORE")
        print("-" * 35)
        stats = summary['score_stats']
        print(f"Promedio:    {stats['mean']:.4f}")
        print(f"Desviación:  {stats['std']:.4f}")
        print(f"Mínimo:      {stats['min']:.4f}")
        print(f"Máximo:      {stats['max']:.4f}")
        print(f"Mediana:     {stats['median']:.4f}")
        
        print(f"\nDECISIONES TOMADAS")
        print("-" * 25)
        for decision, count in summary['decisions'].items():
            percentage = (count / summary['total_processed']) * 100
            print(f"{decision:<12}: {count:>4} ({percentage:>5.1f}%)")
        
        print(f"\nANÁLISIS DEL FEEDBACK LOOP")
        print("-" * 35)
        feedback = summary['feedback_loop_analysis']
        
        print("\nDistribución de reintentos:")
        for intento, count in feedback['reintentos_distribution'].items():
            percentage = (count / summary['total_processed']) * 100
            print(f"  Intento {intento}: {count:>4} mensajes ({percentage:>5.1f}%)")
        
        print("\nEfectividad por intento:")
        for intento, stats in feedback['effectiveness_by_intento'].items():
            print(f"  Intento {intento}: Score promedio = {stats['mean']:.4f} (n={stats['count']})")
        
        print("\nTasa de mejora entre intentos:")
        improvement = feedback['improvement_rate']
        for intento, data in improvement.items():
            if intento > 0 and data.get('improvement_from_previous') is not None:
                change = data['improvement_from_previous']
                arrow = "↑" if change > 0 else "↓"
                print(f"  Intento {intento}: {arrow} {abs(change):.1f}% vs intento {intento-1}")
        
        print("\n" + "="*80)

flink_processor = FlinkProcessor(score_threshold=0.3)  # Threshold más bajo para testing

@app.route('/health', methods=['GET'])
def health():
    model_status = "loaded" if flink_processor.model is not None else "loading"
    summary = flink_processor.metrics.get_summary(flink_processor.score_threshold)
    
    return jsonify({
        "status": "healthy",
        "service": "flink_processor", 
        "score_threshold": flink_processor.score_threshold,
        "model_status": model_status,
        "processing": flink_processor.running,
        "kafka_connected": flink_processor.producer is not None,
        "processed_count": summary.get('total_processed', 0)
    })

@app.route('/analytics', methods=['GET'])
def show_analytics():
    flink_processor.print_analytics()
    return jsonify({"message": "Analytics impresos en consola"})

@app.route('/metrics', methods=['GET'])
def get_metrics():
    summary = flink_processor.metrics.get_summary(flink_processor.score_threshold)
    return jsonify(summary)

@app.route('/update_threshold', methods=['POST'])
def update_threshold():
    data = request.get_json()
    
    if data and 'threshold' in data:
        new_threshold = float(data['threshold'])
        if 0 <= new_threshold <= 1:
            old_threshold = flink_processor.score_threshold
            flink_processor.score_threshold = new_threshold
            return jsonify({
                "success": True,
                "old_threshold": old_threshold,
                "new_threshold": new_threshold
            })
    
    return jsonify({"error": "Threshold debe estar entre 0 y 1"}), 400

@app.route('/stats', methods=['GET'])
def stats():
    summary = flink_processor.metrics.get_summary(flink_processor.score_threshold)
    return jsonify({
        "processed_count": summary.get('total_processed', 0),
        "score_threshold": flink_processor.score_threshold,
        "running": flink_processor.running
    })

@app.route('/reset_metrics', methods=['POST'])
def reset_metrics():
    try:
        metrics_file = '/app/data/flink_metrics.json'
        if os.path.exists(metrics_file):
            os.remove(metrics_file)
        flink_processor.metrics = FlinkMetrics()
        return jsonify({"success": True, "message": "Métricas reseteadas"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/diagnose', methods=['GET'])
def diagnose():
    flink_processor.diagnose_kafka_topics()
    return jsonify({"message": "Diagnóstico ejecutado, ver logs"})

if __name__ == '__main__':
    print("Flink Processor iniciado")
    print(f"Umbral de score: {flink_processor.score_threshold}")
    
    flink_processor.start_processing()
    
    try:
        app.run(host='0.0.0.0', port=8005, debug=False, use_reloader=False)
    except KeyboardInterrupt:

        flink_processor.stop_processing()

