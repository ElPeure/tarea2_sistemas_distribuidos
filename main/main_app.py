import pandas as pd
import requests
import time
import logging
import sys
import json
from kafka import KafkaProducer
import threading
import os

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class MainApp:
    def __init__(self):
        try:
            print("INICIANDO APLICACIÓN PRINCIPAL CON KAFKA")
            
            # Determinar si estamos en Docker
            self.in_docker = os.path.exists('/.dockerenv')
            self.setup_urls()
            
            # Cargar dataset con múltiples rutas posibles
            self.df = self.load_dataset()
            if self.df is None:
                raise Exception("No se pudo cargar el dataset desde ninguna ruta")
                
            print(f"Dataset cargado con {len(self.df)} filas.")
            self.clean_dataset()
            
            print("Inicializando Kafka Producer...")
            # Kafka Producer con manejo de errores
            self.kafka_producer = self.create_kafka_producer()
            if not self.kafka_producer:
                raise Exception("No se pudo crear el productor Kafka")
                
            self.running = True
            self.processed_count = 0
            
            print("MainApp inicializado correctamente")
            
        except Exception as e:
            print(f"Error al inicializar MainApp: {e}")
            sys.exit(1)
    
    def load_dataset(self):
        possible_paths = [
            '/app/preguntas/test.csv',           # Ruta en Docker
            '/preguntas/test.csv',               # Ruta alternativa en Docker  
            './preguntas/test.csv',              # Ruta relativa
            'preguntas/test.csv',                # Ruta desde directorio actual
            '/app/test.csv',                     # Ruta directa
            'test.csv',                          # En directorio actual
        ]
        
        for path in possible_paths:
            try:
                print(f"Intentando cargar dataset desde: {path}")
                if os.path.exists(path):
                    df = pd.read_csv(path)
                    print(f"Dataset cargado exitosamente desde: {path}")
                    return df
                else:
                    print(f"Archivo no existe en: {path}")
            except Exception as e:
                print(f"Error cargando {path}: {e}")
        
        # Si ninguna ruta funciona, mostrar qué archivos existen
        print("Buscando archivos CSV disponibles...")
        try:
            # Listar archivos en directorios posibles
            possible_dirs = ['/app', '/app/preguntas', '/preguntas', '.', './preguntas', '/']
            for dir_path in possible_dirs:
                if os.path.exists(dir_path):
                    print(f"Contenido de {dir_path}:")
                    try:
                        for item in os.listdir(dir_path):
                            print(f"   - {item}")
                    except Exception as e:
                        print(f"  Error listando: {e}")
        except Exception as e:
            print(f"Error listando directorios: {e}")
        
        return None
    
    def setup_urls(self):
        if self.in_docker:
            # Dentro de Docker - usar nombres de servicio
            self.DATABASE_URL = "http://database_service:8001"
            self.CACHE_URL = "http://cache_service:8002"
            self.KAFKA_MANAGER_URL = "http://kafka_manager:8004"
            self.LLM_SERVICE_URL = "http://llm_service:8003"
            self.FLINK_PROCESSOR_URL = "http://flink_processor:8005"
            print("Ejecutando dentro de Docker - usando nombres de servicio")
        else:
            # Fuera de Docker - usar localhost
            self.DATABASE_URL = "http://localhost:8001"
            self.CACHE_URL = "http://localhost:8002"
            self.KAFKA_MANAGER_URL = "http://localhost:8004"
            self.LLM_SERVICE_URL = "http://localhost:8003"
            self.FLINK_PROCESSOR_URL = "http://localhost:8005"
            print("Ejecutando fuera de Docker - usando localhost")
    
    def create_kafka_producer(self):
        max_retries = 10
        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers='kafka:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=30000,
                    retries=3
                )
                print("Productor Kafka creado correctamente")
                return producer
            except Exception as e:
                print(f"Intento {attempt + 1}/{max_retries} - Error creando productor Kafka: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(5)
    
    def clean_dataset(self):
        original_size = len(self.df)
        # Asumiendo que las columnas son: [0, 1, 2, 3] donde 2=pregunta, 3=respuesta_real
        if len(self.df.columns) >= 4:
            self.df = self.df.dropna(subset=[self.df.columns[2], self.df.columns[3]])
            self.df = self.df[self.df[self.df.columns[2]].astype(str).str.strip() != '']
            self.df = self.df[self.df[self.df.columns[3]].astype(str).str.strip() != '']
        cleaned_size = len(self.df)
        
        print(f"Dataset limpiado: {original_size} → {cleaned_size} filas")
        
        if cleaned_size == 0:
            raise Exception("No hay preguntas válidas en el dataset")
    
    def wait_for_services(self, timeout=300):
        print("Verificando servicios...")
        
        services = {
            'database': f"{self.DATABASE_URL}/health",
            'kafka_manager': f"{self.KAFKA_MANAGER_URL}/health",
            'llm_service': f"{self.LLM_SERVICE_URL}/health",
            'flink_processor': f"{self.FLINK_PROCESSOR_URL}/health"
        }
        
        start_time = time.time()
        
        for service_name, url in services.items():
            service_ready = False
            attempt = 0
            
            while time.time() - start_time < timeout and not service_ready:
                attempt += 1
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('status') == 'healthy':
                            print(f"{service_name} listo (intento {attempt}) - {url}")
                            service_ready = True
                            break
                        else:
                            status = data.get('status', 'unknown')
                            print(f"{service_name} no está healthy (status: {status})...")
                    else:
                        print(f"{service_name} respondió con código {response.status_code}...")
                except requests.exceptions.ConnectionError:
                    if attempt % 3 == 0:  # No spammear logs
                        print(f"{service_name} no disponible (intento {attempt}) - {url}")
                except requests.exceptions.Timeout:
                    print(f"{service_name} timeout (intento {attempt})...")
                except Exception as e:
                    if attempt % 3 == 0:
                        print(f"{service_name} error: {str(e)[:50]}...")
                
                if not service_ready:
                    time.sleep(5)
            
            if not service_ready:
                print(f"{service_name} no disponible después de {timeout} segundos")
                return False
        
        print("Todos los servicios están listos!")
        return True
    
    def check_existing_response(self, pregunta):
        try:
            response = requests.get(
                f"{self.DATABASE_URL}/get_respuesta",
                params={"pregunta": pregunta},
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('respuesta')
            return None
        except Exception as e:
            logging.error(f"Error consultando BD: {e}")
            return None
    
    def send_to_kafka_pipeline(self, pregunta, respuesta_real):
        try:
            message = {
                "pregunta_id": f"preg_{int(time.time()*1000)}",
                "pregunta": pregunta,
                "respuesta_real": respuesta_real,
                "timestamp": time.time(),
                "source": "main_app",
                "intento": 0  # Primer intento
            }
            
            # Enviar directamente a Kafka
            self.kafka_producer.send('preguntas-nuevas', message)
            self.kafka_producer.flush()
            
            print(f"Enviado a Kafka: {pregunta[:50]}...")
            return True
            
        except Exception as e:
            logging.error(f"Error enviando a Kafka: {e}")
            return False
    
    def run_continuous_simulation(self, num_preguntas=50, interval_seconds=5):
        print(f"Iniciando simulación con {num_preguntas} preguntas...")
        print(f"Intervalo entre preguntas: {interval_seconds} segundos")
        
        iteration = 0
        valid_questions_processed = 0
        
        while valid_questions_processed < num_preguntas and iteration < num_preguntas * 2:
            iteration += 1
            print(f"\n{'='*50}")
            print(f"ITERACIÓN {iteration} - Procesadas: {valid_questions_processed}/{num_preguntas}")
            print(f"{'='*50}")
            
            # Obtener pregunta aleatoria
            try:
                random_row = self.df.sample(1).iloc[0]
                pregunta = random_row.iloc[2]
                respuesta_real = random_row.iloc[3]
                
                if not self.is_valid_question(pregunta):
                    continue
                
                pregunta_str = str(pregunta).strip()
                respuesta_real_str = str(respuesta_real).strip()
                
                print(f"Pregunta: {pregunta_str}")
                
            except Exception as e:
                print(f"Error al obtener pregunta: {e}")
                continue
            
            # 1. Verificar si ya existe en base de datos
            existing_response = self.check_existing_response(pregunta_str)
            
            if existing_response:
                print(f"Encontrado en BD: {existing_response[:100]}...")
                # Opcional: popular cache
                try:
                    requests.post(
                        f"{self.CACHE_URL}/update",
                        json={"pregunta": pregunta_str, "respuesta": existing_response},
                        timeout=5
                    )
                except:
                    pass
            else:
                # 2. Enviar al pipeline asíncrono de Kafka
                print("No encontrado en BD, enviando a pipeline Kafka...")
                success = self.send_to_kafka_pipeline(pregunta_str, respuesta_real_str)
                if success:
                    self.processed_count += 1
                    print("Enviado a pipeline de procesamiento")
                else:
                    print("Error enviando a pipeline")
            
            valid_questions_processed += 1
            
            # Mostrar estadísticas cada 10 preguntas
            if valid_questions_processed % 10 == 0:
                self.show_stats(valid_questions_processed)
            
            # Esperar antes de la siguiente iteración
            if valid_questions_processed < num_preguntas:
                print(f"Esperando {interval_seconds} segundos...")
                time.sleep(interval_seconds)
        
        print(f"\nProceso finalizado.")
        print(f"Resumen:")
        print(f"   Preguntas válidas procesadas: {valid_questions_processed}")
        print(f"   Preguntas enviadas a Kafka: {self.processed_count}")
        
        # Esperar un poco para que se procesen los mensajes
        print("Esperando procesamiento de mensajes en Kafka...")
        time.sleep(10)
        
        return self.processed_count
    
    def is_valid_question(self, pregunta):
        if pd.isna(pregunta):
            return False
        pregunta_str = str(pregunta).strip()
        invalid_values = ['', 'nan', 'NaN', 'None', 'null']
        return pregunta_str not in invalid_values and len(pregunta_str) >= 3
    
    def show_stats(self, num_preguntas):
        try:
            print(f"\n --- ESTADÍSTICAS después de {num_preguntas} preguntas ---")
            
            # Estadísticas de cache
            try:
                cache_response = requests.get(f"{self.CACHE_URL}/stats", timeout=5)
                if cache_response.status_code == 200:
                    cache_stats = cache_response.json()
                    print(f"CACHE LRU: Hits {cache_stats['lru']['hit_rate']:.1f}%")
            except:
                print("CACHE: No disponible")
            
            # Promedio de scores
            try:
                avg_response = requests.get(f"{self.DATABASE_URL}/average_score", timeout=5)
                if avg_response.status_code == 200:
                    avg_data = avg_response.json()
                    avg_score = avg_data.get('average_score', 0)
                    print(f"Promedio de score: {avg_score:.4f}")
            except:
                print("Score promedio: No disponible")
            
            # Topics de Kafka
            try:
                topics_response = requests.get(f"{self.KAFKA_MANAGER_URL}/topics", timeout=5)
                if topics_response.status_code == 200:
                    topics_data = topics_response.json()
                    print(f"Topics activos: {len(topics_data.get('topics', []))}")
            except:
                print("Topics: No disponible")
            
            print("-----------------------------------------")
            
        except Exception as e:
            logging.error(f"Error al obtener estadísticas: {e}")

    def generate_final_report(self):
        import requests
        import time
        
        print("\n" + "="*100)
        print("REPORTE FINAL DEL SISTEMA - RESUMEN DE MÉTRICAS")
        print("="*100)
        
        # Esperar a que se procesen los últimos mensajes
        print("Generando reporte final...")
        time.sleep(5)
        
        services = {
            'kafka_metrics': f"{self.KAFKA_MANAGER_URL}/metrics",
            'flink_metrics': f"{self.FLINK_PROCESSOR_URL}/metrics", 
            'db_stats': f"{self.DATABASE_URL}/detailed_stats"
        }
        
        try:
            # Obtener métricas de Kafka
            print("\nObteniendo métricas de Kafka...")
            kafka_response = requests.get(services['kafka_metrics'], timeout=15)
            if kafka_response.status_code == 200:
                kafka_metrics = kafka_response.json()
                
                print("\nKAFKA - MÉTRICAS DE THROUGHPUT")
                print("-" * 50)
                print(f"Total mensajes: {kafka_metrics.get('total_messages', 0)}")
                print(f"Tiempo total: {kafka_metrics.get('total_processing_time_seconds', 0):.2f}s")
                
                throughput = kafka_metrics.get('throughput_per_minute', {})
                if throughput:
                    print("\nThroughput por topic:")
                    for topic, rate in throughput.items():
                        print(f"  {topic:<25}: {rate:>6.2f} msg/min")
                
                # Tiempos en cola
                queue_times = kafka_metrics.get('queue_times_seconds', {})
                if queue_times:
                    print("\nTIEMPOS PROMEDIO EN COLA:")
                    for topic, times in queue_times.items():
                        print(f"  {topic:<25}: {times.get('avg', 0):.2f}s")
            else:
                print(f"Kafka metrics respondió con código: {kafka_response.status_code}")
            
            # Obtener analytics de Flink
            print("\nObteniendo analytics de Flink...")
            flink_response = requests.get(services['flink_metrics'], timeout=15)
            if flink_response.status_code == 200:
                flink_metrics = flink_response.json()
                
                print("\nFLINK - EFECTIVIDAD DEL FEEDBACK LOOP")
                print("-" * 45)
                print(f"Umbral de score: {flink_metrics.get('score_threshold', 0)}")
                print(f"Total procesado: {flink_metrics.get('total_processed', 0)}")
                
                decisions = flink_metrics.get('decisions', {})
                if decisions:
                    total = sum(decisions.values())
                    print("\nDecisiones tomadas:")
                    for decision, count in decisions.items():
                        percentage = (count / total) * 100 if total > 0 else 0
                        print(f"  {decision:<12}: {count:>4} ({percentage:>5.1f}%)")
                
                # Mostrar efectividad de reintentos
                feedback = flink_metrics.get('feedback_loop_analysis', {})
                effectiveness = feedback.get('effectiveness_by_intento', {})
                if effectiveness:
                    print("\nEFECTIVIDAD POR INTENTO:")
                    for intento, stats in effectiveness.items():
                        print(f"  Intento {intento}: Score promedio = {stats['mean']:.4f} (n={stats['count']})")
            else:
                print(f"Flink metrics respondió con código: {flink_response.status_code}")
            
            # Verificar base de datos con estadísticas detalladas
            print("\nVerificando base de datos...")
            db_response = requests.get(services['db_stats'], timeout=15)
            if db_response.status_code == 200:
                db_stats = db_response.json()
                print(f"\nDATABASE - RESPUESTAS ALMACENADAS")
                print("-" * 45)
                print(f"Total respuestas guardadas: {db_stats.get('total_responses', 0)}")
                print(f"Preguntas únicas procesadas: {db_stats.get('unique_questions', 0)}")
                print(f"Score promedio: {db_stats.get('average_score', 0):.4f}")
                
                # Distribución de scores
                score_dist = db_stats.get('score_distribution', [])
                if score_dist:
                    print(f"\nDISTRIBUCIÓN DE SCORES:")
                    for dist in score_dist:
                        print(f"  {dist['range']:<20}: {dist['count']:>4} respuestas")
            else:
                print(f"Database stats respondió con código: {db_response.status_code}")
            
        except requests.exceptions.ConnectionError as e:
            print(f"No se pudo conectar a los servicios: {e}")
            print(f"URLs intentadas:")
            for service, url in services.items():
                print(f"   {service}: {url}")
        except Exception as e:
            print(f"Error generando reporte: {e}")
        
        print("\n" + "="*100)
        print("PROCESAMIENTO COMPLETADO")
        print("="*100)

if __name__ == '__main__':
    try:
        app = MainApp()
        
        if app.wait_for_services():
            print("\nTODOS LOS SERVICIOS LISTOS")
            print("INICIANDO SIMULACIÓN...")
            
            total_sent = app.run_continuous_simulation(50, 5)
            print(f"Simulación completada. Total enviado: {total_sent}")

            # Generar reporte final
            app.generate_final_report()
            
        else:
            print("NO SE PUDIERON INICIALIZAR TODOS LOS SERVICIOS")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nAplicación interrumpida por el usuario")
        # Generar reporte final incluso si se interrumpe
        try:
            app.generate_final_report()
        except:
            pass
    except Exception as e:
        print(f"Error fatal en MainApp: {e}")
        sys.exit(1)