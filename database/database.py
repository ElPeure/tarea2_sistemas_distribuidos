import mysql.connector
from mysql.connector import Error
import time
import logging
from flask import Flask, request, jsonify
import json
from kafka import KafkaConsumer
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DatabaseService')

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.kafka_consumer_active = False
        self.connect_with_retry()
        self.start_kafka_consumer()
    
    def connect_with_retry(self, max_retries=30, delay=5):
        logger.info("Iniciando servicio de base de datos...")
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Intento {attempt}/{max_retries}: Conectando a MySQL...")
                
                self.conn = mysql.connector.connect(
                    host='mysql_base',
                    user='usuario',
                    password='pass123',
                    database='mi_base',
                    autocommit=True,
                    connection_timeout=10
                )
                
                if self.conn.is_connected():
                    db_info = self.conn.get_server_info()
                    logger.info(f"Conectado a MySQL Server v{db_info}")
                    self.init_database()
                    return True
                    
            except Error as e:
                logger.warning(f"Intento {attempt}/{max_retries} falló: {e}")
                if attempt == max_retries:
                    logger.error("No se pudo conectar a MySQL después de todos los intentos")
                    return False
                logger.info(f"Esperando {delay} segundos...")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                if attempt == max_retries:
                    return False
                time.sleep(delay)
        
        return False
    
    def init_database(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS respuestas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    pregunta TEXT NOT NULL,
                    respuesta_dataset TEXT,
                    respuesta_llm TEXT,
                    score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_pregunta (pregunta(255))
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """)
            cursor.close()
            logger.info("Tabla 'respuestas' verificada/creada")
        except Exception as e:
            logger.error(f"Error al inicializar BD: {e}")

    def health_check(self):
        try:
            if self.conn is None or not self.conn.is_connected():
                logger.warning("Conexión MySQL no disponible")
                return False
            
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone() 
            cursor.close()  
            
            return True
        except Exception as e:
            logger.error(f"Health check falló: {e}")
            return False
    
    def insert_respuesta(self, pregunta, respuesta_dataset, respuesta_llm, score):
        try:
            if not self.health_check():
                return False
                
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO respuestas (pregunta, respuesta_dataset, respuesta_llm, score) VALUES (%s, %s, %s, %s)",
                (pregunta, respuesta_dataset, respuesta_llm, score)
            )
            cursor.close()
            logger.info(f"Insertado en BD: {pregunta[:30]}...")
            return True
        except Exception as e:
            logger.error(f"Error al insertar: {e}")
            return False
    
    def get_respuesta_from_db(self, pregunta):
        try:
            if not self.health_check():
                return None
                
            cursor = self.conn.cursor()
            cursor.execute("SELECT respuesta_llm FROM respuestas WHERE pregunta = %s", (pregunta,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.info(f"Encontrado en BD: {pregunta[:30]}...")
                return result[0]
            return None
        except Exception as e:
            logger.error(f"Error al buscar: {e}")
            return None
    
    def get_average_score(self):
        try:
            if not self.health_check():
                return None
                
            cursor = self.conn.cursor()
            cursor.execute("SELECT AVG(score) FROM respuestas WHERE score IS NOT NULL")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else 0.0
        except Exception as e:
            logger.error(f"Error al obtener promedio: {e}")
            return 0.0

    def get_detailed_stats(self):
        try:
            if not self.health_check():
                return None
                
            cursor = self.conn.cursor()
            
            # Total de respuestas
            cursor.execute("SELECT COUNT(*) as total FROM respuestas")
            total_responses = cursor.fetchone()[0]
            
            # Preguntas únicas (basado en contenido de pregunta)
            cursor.execute("SELECT COUNT(DISTINCT pregunta) as unique_questions FROM respuestas")
            unique_questions = cursor.fetchone()[0]
            
            # Score promedio
            cursor.execute("SELECT AVG(score) as avg_score FROM respuestas WHERE score IS NOT NULL")
            avg_score_result = cursor.fetchone()
            avg_score = float(avg_score_result[0]) if avg_score_result[0] is not None else 0.0
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as count,
                    CASE 
                        WHEN score >= 0.8 THEN 'Excelente (0.8-1.0)'
                        WHEN score >= 0.6 THEN 'Bueno (0.6-0.79)'
                        WHEN score >= 0.4 THEN 'Regular (0.4-0.59)'
                        ELSE 'Bajo (<0.4)'
                    END as score_range
                FROM respuestas 
                WHERE score IS NOT NULL
                GROUP BY score_range
                ORDER BY score_range
            """)
            score_distribution = cursor.fetchall()
            
            cursor.close()
            
            return {
                "total_responses": total_responses,
                "unique_questions": unique_questions,
                "average_score": avg_score,
                "score_distribution": [
                    {"range": row[1], "count": row[0]} for row in score_distribution
                ],
                "database": "operational"
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas detalladas: {e}")
            return None

    def start_kafka_consumer(self):
        def consume_validadas():
            logger.info("🔄 Iniciando consumidor para 'respuestas-validadas'...")
            
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    consumer = KafkaConsumer(
                        'respuestas-validadas',
                        bootstrap_servers='kafka:9092',
                        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                        group_id='database-consumer',
                        auto_offset_reset='earliest'
                    )
                    
                    self.kafka_consumer_active = True
                    logger.info("Consumidor listo para 'respuestas-validadas'")
                    
                    for message in consumer:
                        try:
                            data = message.value
                            pregunta_id = data.get('pregunta_id', 'unknown')
                            score = data.get('score_calculado', 0)
                            
                            logger.info(f"PERSISTIENDO - ID: {pregunta_id} - Score: {score:.4f}")
                            
                            # Insertar en base de datos
                            success = self.insert_respuesta(
                                data.get('pregunta', ''),
                                data.get('respuesta_real', ''),
                                data.get('respuesta_llm', ''),
                                score
                            )
                            
                            if success:
                                logger.info(f"PERSISTIDO EN BD - ID: {pregunta_id}")
                            else:
                                logger.error(f"ERROR PERSISTIENDO - ID: {pregunta_id}")
                                
                        except Exception as e:
                            logger.error(f"Error procesando mensaje: {e}")
                    break
                            
                except Exception as e:
                    logger.warning(f"Intento {attempt + 1}/{max_retries} - Error en consumidor: {e}")
                    if attempt == max_retries - 1:
                        logger.error("No se pudo conectar a Kafka después de todos los intentos")
                        self.kafka_consumer_active = False
                    time.sleep(10)
        
        thread = threading.Thread(target=consume_validadas)
        thread.daemon = True
        thread.start()
        logger.info("Consumidor Kafka iniciado")

app = Flask(__name__)
db_manager = DatabaseManager()

@app.route('/health', methods=['GET'])
def health():
    is_healthy = db_manager.health_check()
    status_code = 200 if is_healthy else 503
    
    return jsonify({
        "status": "healthy" if is_healthy else "unhealthy",
        "service": "database",
        "mysql_connected": is_healthy,
        "kafka_consumer": db_manager.kafka_consumer_active,
        "timestamp": time.time()
    }), status_code

@app.route('/insert', methods=['POST'])
def insert():
    try:
        data = request.get_json()
        if not data or 'pregunta' not in data:
            return jsonify({"success": False, "error": "Datos incompletos"}), 400
        
        success = db_manager.insert_respuesta(
            data.get('pregunta'),
            data.get('respuesta_dataset'),
            data.get('respuesta_llm'),
            data.get('score', 0.0)
        )
        return jsonify({"success": success})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/get_respuesta', methods=['GET'])
def get_respuesta():
    pregunta = request.args.get('pregunta')
    if not pregunta:
        return jsonify({"error": "Parámetro 'pregunta' requerido"}), 400
    
    respuesta = db_manager.get_respuesta_from_db(pregunta)
    return jsonify({"respuesta": respuesta})

@app.route('/average_score', methods=['GET'])
def average_score():
    promedio = db_manager.get_average_score()
    return jsonify({"average_score": promedio})

@app.route('/stats', methods=['GET'])
def stats():
    try:
        stats_data = db_manager.get_detailed_stats()
        if stats_data:
            return jsonify(stats_data)
        else:
            return jsonify({"error": "No se pudieron obtener las estadísticas"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/detailed_stats', methods=['GET'])
def detailed_stats():
    try:
        stats_data = db_manager.get_detailed_stats()
        if stats_data:
            return jsonify(stats_data)
        else:
            return jsonify({"error": "No se pudieron obtener las estadísticas"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print(" Esperando consultas HTTP y mensajes Kafka...")

    app.run(host='0.0.0.0', port=8001, debug=False)
