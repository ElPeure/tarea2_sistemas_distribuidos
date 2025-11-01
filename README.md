## Tarea 2: Sistema de la tarea 1 complementado con flink y kafka + cambio de arquitectura

Este documento describe los pasos necesarios para la correcta configuración y ejecución del proyecto de Sistemas Distribuidos, asegurando que todos los componentes estén listos.

---

## Requisitos y Preparación

Para ejecutar este proyecto sin fallos, es imprescindible cumplir con los siguientes requisitos previos:

### Clonar repositorio
git clone [https://github.com/ElPeure/Flint_Kafka](https://github.com/ElPeure/tarea2_sistemas_distribuidos.git)
cd Flint_Kafka

### Desplegar todos los servicios
docker-compose up -d

### Verificar estado de servicios
curl http://localhost:8001/health   (Database)

curl http://localhost:8003/health  (LLM Service) 

curl http://localhost:8005/health  (Flink Processor)

### Iniciar procesamiento
curl -X POST http://localhost:8000/iniciar

- Una vez completados los pasos de configuración y asegurado el espacio de almacenamiento, el programa puede ser ejecutado sin riesgo de fallos por dependencias.
- El presente informe pero sin el archivo test.csv debido a su tamaño.
