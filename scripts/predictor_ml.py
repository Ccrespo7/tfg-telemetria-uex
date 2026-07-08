

import json
import time
import datetime
import csv
import threading
import warnings
import joblib
import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt
import queue
from pathlib import Path

warnings.filterwarnings("ignore", category=UserWarning, message=".*sklearn.*")
warnings.filterwarnings("ignore", category=UserWarning, message=".*Parallel.*")

import os

# ─────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────
MQTT_BROKER     = os.environ.get("MQTT_BROKER", "100.114.60.121")
MQTT_PORT       = 1883
TOPIC_ENTRADA   = "uex/telemetria/skoda"
TOPIC_SALIDA    = "uex/predicciones/skoda"
CARPETA_MODELOS = Path(__file__).resolve().parent.parent / "modelos"
CARPETA_DATOS   = Path(__file__).resolve().parent.parent / "datos"

# Features exactas con las que se entrenó cada modelo
FEATURES_M1 = ["rpm", "velocidad", "pos_acelerador", "ratio_rpm_vel"]
FEATURES_M3 = ["rpm", "velocidad", "pos_acelerador", "temp_motor"]
FEATURES_M4 = ["rpm", "velocidad", "pos_acelerador", "temp_motor"]

# Valores por defecto para parámetros opcionales
DEFAULTS_OPCIONALES = {}

# ─────────────────────────────────────────
# Cargar modelos
# ─────────────────────────────────────────
def cargar_modelos():
    print("Cargando modelos ML (M1: Marchas, M3: Temperatura)...")
    modelos = {
        "m1":      joblib.load(CARPETA_MODELOS / "modelo1_marcha.pkl"),
        "m3":      joblib.load(CARPETA_MODELOS / "modelo3_temperatura.pkl"),
        "scaler3": joblib.load(CARPETA_MODELOS / "scaler_modelo3.pkl"),
    }
    print("Modelos M1 y M3 cargados.")
    
    # M4 depende de si el usuario ha compilado el jupyter
    try:
        modelos["m4"] = joblib.load(CARPETA_MODELOS / "modelo4_anomalia.pkl")
        modelos["scaler4"] = joblib.load(CARPETA_MODELOS / "scaler_anomalia.pkl")
        print("Modelo M4 Anomalías (Isolation Forest) cargado.")
    except Exception:
        print(" M4 no cargado. Revisar su .pkl")
        modelos["m4"] = None

    for key, mod in modelos.items():
        if hasattr(mod, "n_jobs"):
            mod.n_jobs = 1

    return modelos


# ─────────────────────────────────────────
# Caché de últimos valores conocidos
# ─────────────────────────────────────────
ultimo_conocido: dict = dict(DEFAULTS_OPCIONALES)


def resolver(datos: dict, campo: str) -> float:
    val = datos.get(campo)
    if val is not None:
        try:
            f = float(val)
            ultimo_conocido[campo] = f
            return f
        except (TypeError, ValueError):
            pass
    return ultimo_conocido.get(campo, DEFAULTS_OPCIONALES.get(campo, 0.0))


def predecir(datos: dict, modelos: dict) -> dict | None:
    # Campos obligatorios
    campos_obligatorios = ["rpm", "velocidad", "temp_motor",
                           "pos_acelerador"]
    if not all(c in datos for c in campos_obligatorios):
        return None

    # Propagar timestamp original del coche
    ts_origen = datos.get("timestamp", int(time.time_ns()))

    rpm   = float(datos["rpm"])
    vel   = float(datos["velocidad"])
    temp  = float(datos["temp_motor"])
    accel = float(datos["pos_acelerador"])

    ratio  = rpm / vel if vel > 2 else 0.0

    # ── Modelo 1: Marcha ──────────────────────────────────────────
    X1 = pd.DataFrame(
        [[rpm, vel, accel, ratio]],
        columns=FEATURES_M1
    )
    # Optimización: LLamar a predict_proba una sola vez
    proba_todas = modelos["m1"].predict_proba(X1)[0]
    marcha_pred = int(modelos["m1"].classes_[np.argmax(proba_todas)])
    proba_m1    = float(np.max(proba_todas))

    # ── Modelo 3: Temperatura futura (~1 segundo) ─────────────────
    X3_raw = pd.DataFrame(
        [[rpm, vel, accel, temp]],
        columns=FEATURES_M3
    )
    X3_scaled   = modelos["scaler3"].transform(X3_raw)
    temp_futura = float(modelos["m3"].predict(X3_scaled)[0])
    delta_temp  = round(temp_futura - temp, 3)

    # ── Modelo 4: Anomalías (Isolation Forest) ────────────────────
    es_anomalia = 0
    if modelos.get("m4") is not None:
        X4_raw = pd.DataFrame(
            [[rpm, vel, accel, temp]],
            columns=FEATURES_M4
        )
        X4_scaled = modelos["scaler4"].transform(X4_raw)
        pred_anom = modelos["m4"].predict(X4_scaled)[0]
        # Isolation Forest devuelve -1 si es anomalía, 1 si es normal
        es_anomalia = 1 if pred_anom == -1 else 0

    return {
        # Timestamp original del coche 
        "timestamp":    ts_origen,
        # Predicciones
        "marcha_pred":  marcha_pred,
        "marcha_conf":  round(proba_m1, 3),
        "temp_pred_1s": round(temp_futura, 2),
        "delta_temp":   delta_temp,
        "es_anomalia":  es_anomalia,
        # Datos originales reenviados
        "rpm":            rpm,
        "velocidad":      vel,
        "temp_motor":     temp,
        "pos_acelerador": accel,
    }


# MQTT + Hilos
# ─────────────────────────────────────────
ultimo_mensaje = None
mensaje_lock   = threading.Lock()
hay_mensaje    = threading.Event()

# Cola asíncrona para no bloquear el hilo de predicción por red lenta
cola_mqtt = queue.Queue(maxsize=50)

modelos_globales = {}
cliente_pub      = None
csv_writer       = None
archivo_csv      = None
stats = {"recibidos": 0, "publicados": 0, "descartados": 0, "errores": 0, "drops_red": 0}

def hilo_publicador_mqtt():
    """Consume la cola asíncrona y lo envía. Evita bloqueos TCP."""
    while True:
        try:
            payload = cola_mqtt.get(timeout=0.5)
            cliente_pub.publish(TOPIC_SALIDA, json.dumps(payload), qos=0)
        except queue.Empty:
            continue
        except Exception as e:
            stats["drops_red"] += 1



def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(TOPIC_ENTRADA)
        print(f"Conectado a MQTT. Escuchando: {TOPIC_ENTRADA}")
    else:
        print(f"Error conexión MQTT: {reason_code}")


def on_message(client, userdata, msg):
    global ultimo_mensaje
    try:
        datos = json.loads(msg.payload.decode())
        with mensaje_lock:
            if ultimo_mensaje is not None:
                stats["descartados"] += 1
            ultimo_mensaje = datos
            stats["recibidos"] += 1
        hay_mensaje.set()
    except Exception as e:
        stats["errores"] += 1
        print(f"Error parseando mensaje: {e}")


def hilo_prediccion():
    global ultimo_mensaje

    while True:
        hay_mensaje.wait()

        with mensaje_lock:
            datos = ultimo_mensaje
            ultimo_mensaje = None
        hay_mensaje.clear()

        if datos is None:
            continue

        try:
            t_inicio  = time.perf_counter()
            resultado = predecir(datos, modelos_globales)

            if resultado is None:
                stats["errores"] += 1
                continue

            # Encolar para publicador async en vez de bloquear
            try:
                cola_mqtt.put_nowait(resultado)
                stats["publicados"] += 1
            except queue.Full:
                stats["drops_red"] += 1
                try: cola_mqtt.get_nowait()
                except: pass
                cola_mqtt.put_nowait(resultado)
                
            t_pred = (time.perf_counter() - t_inicio) * 1000  # ms

            # Log en consola
            if stats["publicados"] % 10 == 0:
                anom_mark = "!ANOMALÍA!" if resultado["es_anomalia"] == 1 else "Normal"
                print(
                    f"[{stats['publicados']:5d}] "
                    f"v={resultado['velocidad']:5.1f} | "
                    f"M{resultado['marcha_pred']} ({resultado['marcha_conf']*100:.0f}%) | "
                    f"T_pred={resultado['temp_pred_1s']:.1f}°C | "
                    f"M4:{anom_mark} | "
                    f"{t_pred:.0f}ms skip={stats['descartados']} net_drops={stats['drops_red']}"
                )

        except Exception as e:
            stats["errores"] += 1
            print(f"Error procesando: {e}")


def main():
    global modelos_globales, cliente_pub, csv_writer, archivo_csv

    print("=" * 60)
    print("  Predictor ML – UEx Motorsport TFG")
    print("=" * 60)

    modelos_globales = cargar_modelos()

    fecha_actual = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_csv   = CARPETA_DATOS / f"predicciones_skoda_{fecha_actual}.csv"
    archivo_csv  = open(nombre_csv, mode='w', newline='')
    csv_writer   = csv.writer(archivo_csv, delimiter=';')
    csv_writer.writerow([
        "timestamp", "timestamp_iso",
        "rpm", "velocidad", "temp_motor", "pos_acelerador",
        "marcha_pred", "marcha_conf_pct",
        "temp_pred_1s", "delta_temp",
        "es_anomalia"
    ])

    cliente_pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="predictor_pub")
    cliente_pub.max_queued_messages_set(20)
    cliente_pub.connect(MQTT_BROKER, MQTT_PORT, 60)
    cliente_pub.loop_start()

    # Arrancar hilo publicador async MQTT
    t_pub = threading.Thread(target=hilo_publicador_mqtt, daemon=True)
    t_pub.start()

    t = threading.Thread(target=hilo_prediccion, daemon=True)
    t.start()

    cliente_sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="predictor_sub")
    cliente_sub.on_connect = on_connect
    cliente_sub.on_message = on_message
    cliente_sub.connect(MQTT_BROKER, MQTT_PORT, 60)

    print(f"\nPublicando predicciones en: {TOPIC_SALIDA}")
    print("   Pulsa Ctrl+C para detener.\n")

    try:
        cliente_sub.loop_forever()
    except KeyboardInterrupt:
        print(f"\n Detenido.")
    finally:
        cliente_sub.disconnect()
        cliente_pub.disconnect()
        if archivo_csv:
            archivo_csv.flush()
            archivo_csv.close()

if __name__ == "__main__":
    main()