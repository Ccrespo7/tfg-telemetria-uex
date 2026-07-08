# Monitorización Inteligente de Vehículos mediante Ingeniería de Datos

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-2.8-blue?style=flat&logo=influxdb)
![Grafana](https://img.shields.io/badge/grafana-%23F46800.svg?style=flat&logo=grafana&logoColor=white)
![MQTT](https://img.shields.io/badge/mqtt-Mosquitto-green)
![Scikit-Learn](https://img.shields.io/badge/scikit--learn-%23F7931E.svg?style=flat&logo=scikit-learn&logoColor=white)

Este repositorio contiene el código fuente y la configuración de infraestructura para el Trabajo de Fin de Grado desarrollado para el equipo universitario **UEx Motorsport**. El proyecto consiste en una arquitectura integral de telemetría en tiempo real basada en conceptos de Ingeniería de Datos.

## Descripción del Proyecto

El sistema captura datos físicos de un vehículo a través de su puerto OBD-II utilizando una Raspberry Pi, los envía en tiempo real (aprox. 50 Hz) mediante el protocolo MQTT sobre redes 4G y los almacena en una base de datos de series temporales (InfluxDB) en un servidor.

Paralelamente, una serie de modelos de **Machine Learning** procesan estos datos en directo para predecir variables clave:
- **Predictor de Marcha:** Clasificador (Random Forest) para inferir la marcha engranada.
- **Gemelo Digital Térmico:** Regresor (Gradient Boosting) para anticipar anomalías de temperatura.
- **Detector de Anomalías:** Algoritmo no supervisado (Isolation Forest) para detectar comportamientos mecánicos anómalos.

Finalmente, todos los datos y predicciones se visualizan en un panel interactivo de Grafana.

## Arquitectura del Sistema

La arquitectura está completamente contenerizada y se divide en:
1. **Adquisición (Coche):** Script de Python (`obd2_monitor.py`) en Raspberry Pi.
2. **Mensajería:** Bróker MQTT (Eclipse Mosquitto).
3. **Ingesta:** Telegraf.
4. **Almacenamiento:** InfluxDB v2.
5. **Inteligencia Artificial:** Script predictor desplegado como contenedor Docker propio.
6. **Visualización:** Dashboard de Grafana.

## Estructura del Repositorio

- `/scripts`: Scripts de adquisición de datos en la Raspberry Pi y el script predictor de IA.
- `/modelos`: Modelos de Machine Learning entrenados y exportados (`.pkl`).
- `/notebooks`: Jupyter Notebooks utilizados para la exploración de datos y el entrenamiento de los modelos.
- `/telegraf`: Archivos de configuración del agente Telegraf.
- `/mosquitto`: Configuración del bróker MQTT.
- `docker-compose.yml`: Archivo de orquestación para desplegar toda la infraestructura del servidor.
- `Dockerfile.predictor`: Imagen Docker personalizada para el módulo de IA.
- `requirements.txt`: Dependencias de Python necesarias.

## Despliegue

Toda la infraestructura del servidor se puede levantar fácilmente utilizando Docker Compose. Clona el repositorio y ejecuta:

```bash
docker compose up -d --build
```

Esto levantará los 5 contenedores necesarios (Mosquitto, InfluxDB, Grafana, Telegraf y el Predictor IA). 

Para comenzar a inyectar datos desde el vehículo, ejecutar el script de monitorización:

```bash
cd scripts
python3 obd2_monitor.py
```
