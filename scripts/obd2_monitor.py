"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                     MONITOR OBD2 CON MQTT Y CSV                               ║
╚════════════════════════════════════════════════════════════════════════════════╝

CONFIGURACIÓN:
  - MQTT_BROKER: Servidor remoto para publicación de datos
  - PARAMETROS_RAPIDOS: RPM, velocidad, acelerador (0.01s)
  - PARAMETROS_LENTOS: Temperatura del motor (1.0s)
"""

import time
import datetime
import csv
import json
import queue
import threading
import obd
import paho.mqtt.client as mqtt
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich import box

CARPETA_DATOS = Path(__file__).resolve().parent.parent / "datos"

console = Console()

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN MQTT
# ═════════════════════════════════════════════════════════════════════════════
# Parámetros de conexión al broker MQTT remoto para publicación de telemetría
#
MQTT_BROKER = "100.114.60.121"  # IP del servidor MQTT
MQTT_PORT = 1883                 # Puerto estándar MQTT
MQTT_TOPIC = "uex/telemetria/skoda"  # Topic donde se publican los datos
MQTT_QOS = 1                     # Calidad de Servicio: 1 = con ACK del broker
                                  # (Cambiar a 0 si la latencia es crítica)
MQTT_MAX_COLA = 50               # Tamaño máximo de la cola en memoria
                                  # Si se acumulan >50 msgs sin enviar (~5s),
                                  # se descartan los más viejos

# ═════════════════════════════════════════════════════════════════════════════
# PARÁMETROS OBD2 MULTIPLEXADOS (PIDs del vehículo)
# ═════════════════════════════════════════════════════════════════════════════
# Sensores a leer del vehículo. Los "rápidos" se leen en cada iteración (~0.01s).
# Los "lentos" se leen cada MULTIPLEX_INTERVALO (1.0s).
#
# Formato: (comando_OBD, etiqueta_pantalla, unidad_visualización, nombre_BD)
#
PARAMETROS_RAPIDOS = [
    (obd.commands.RPM,             "RPM",              "rpm",  "rpm"),
    (obd.commands.SPEED,           "Velocidad",        "km/h", "velocidad"),
    (obd.commands.THROTTLE_POS,    "Pos. Acelerador",  "%",    "pos_acelerador"),
]

PARAMETROS_LENTOS = [
    (obd.commands.COOLANT_TEMP,    "Temp. Motor",      "°C",   "temp_motor"),
]

PARAMETROS = PARAMETROS_RAPIDOS + PARAMETROS_LENTOS  # Lista completa para iteración

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE RENDIMIENTO Y THROTTLING
# ═════════════════════════════════════════════════════════════════════════════
# Controles para optimizar el rendimiento sin saturar la red ni el disco
#
CSV_FLUSH_CADA = 10              # Flush a disco cada N filas (reduce syscalls)
RICH_REFRESH_HZ = 4              # Hz de actualización de la interfaz (4 FPS)
MAX_CSV_HZ = 10                  # Máximo 10 Hz en CSV (evita datos redundantes)
MIN_INTERVALO_CSV = 1.0 / MAX_CSV_HZ  # ~100ms entre escrituras CSV
MULTIPLEX_INTERVALO = 1.0        # PIDs lentos cada 1.0 segundo

# ═════════════════════════════════════════════════════════════════════════════
# ESTADO GLOBAL COMPARTIDO ENTRE HILOS
# ═════════════════════════════════════════════════════════════════════════════
# Sincronización thread-safe de datos OBD2 entre el hilo lector y el bucle principal
#
valores_cache = {}               # Dict: {etiqueta → (valor, unidad, nombre_BD)}
                                  # Ej: {"RPM": (2500, "rpm", "rpm")}
valores_lock = threading.Lock()  # Lock para acceso seguro a valores_cache

hilo_lector_activo = True        # Flag: controla si el hilo OBD2 sigue ejecutándose
                                  # Se pone a False al pulsar Ctrl+C

# ═════════════════════════════════════════════════════════════════════════════
# MÉTRICAS DE RED (DIAGNÓSTICO MQTT)
# ═════════════════════════════════════════════════════════════════════════════
# Estadísticas en tiempo real de la conexión MQTT para visualización en el panel
#
net_stats = {
    "enviados": 0,               # Contador total de mensajes publicados exitosamente
    "drops": 0,                  # Contador de mensajes descartados por cola llena
    "ultimo_rtt_ms": 0.0,        # RTT (Round-Trip Time) del último publish en ms
    "cola_actual": 0,            # Tamaño actual de la cola MQTT (msgs pendientes)
}
net_stats_lock = threading.Lock()  # Lock para acceso seguro a net_stats


def conectar_obd_sync(port: str | None = None) -> obd.OBD:
    """
    Establecer conexión SÍNCRONA con el adaptador OBD2 del vehículo.
    
    Esta función intenta conectar al puerto serial del adaptador OBD2 en modo
    síncrono (bloqueante). Si no se especifica puerto, obd.OBD intenta auto-detectar.
    
    Args:
        port (str | None): Puerto serial del adaptador (ej: "COM3", "/dev/ttyUSB0").
                          Si None, se intenta auto-detectar.
    
    Returns:
        obd.OBD: Objeto de conexión establecida al vehículo.
    
    Raises:
        ConnectionError: Si no se encuentra el adaptador o el vehículo está apagado.
    
    Notas:
        • fast=True: Usa protocolos OBD2 optimizados para velocidad
        • timeout=1: Timeout de 1s por query al vehículo
    """
    console.print("[bold yellow]Conectando al adaptador OBD2...[/bold yellow]")
    connection = obd.OBD(portstr=port, fast=True, timeout=1)
    if connection.is_connected():
        console.print(f"[bold green]OBD2 Conectado en: {connection.port_name()}[/bold green]")
    else:
        raise ConnectionError("Adaptador OBD2 no encontrado o coche apagado.")
    return connection


def hilo_lector_obd(connection: obd.OBD):
    """
    Hilo trabajador: lectura continua de sensores OBD2 del vehículo.
    
    Implementa un sistema MULTIPLEXADO de lectura:
    • RÁPIDOS (PIDs simples): RPM, velocidad, acelerador → cada 0.01s (100 Hz)
    • LENTOS (PIDs complejos): temperatura del motor → cada 1.0s
    
    Los valores se almacenan en valores_cache de forma thread-safe para que
    el hilo principal y MQTT puedan leerlos sin bloqueos.
    
    Flujo:
      1. Inicializa caché con todos los PIDs en None
      2. Entra en bucle infinito (mientras hilo_lector_activo sea True)
      3. Lee PIDs rápidos en cada iteración
      4. Lee PIDs lentos cada 1 segundo
      5. Actualiza valores_cache con lock
      6. Duerme 10ms entre iteraciones (ahorra CPU)
    
    Tolerancia a errores:
      • Si un query falla, se continúa con el siguiente
      • Los valores None indican "sin dato disponible"
      • No propagra excepciones (continúa aunque haya errores)
    
    Args:
        connection (obd.OBD): Objeto de conexión al vehículo
    """
    ultimo_lento = 0.0  # Timestamp del último poll de PIDs lentos
    
    # Inicializar caché con la estructura de todos los parámetros
    with valores_lock:
        for cmd, etiqueta, unidad, db in PARAMETROS:
            valores_cache[etiqueta] = (None, unidad, db)

    while hilo_lector_activo:
        # ─────────────────────────────────────────────────────────────
        # LECTURA RÁPIDA: Se ejecuta cada 10ms (~100 Hz)
        # ─────────────────────────────────────────────────────────────
        for cmd, etiqueta, unidad, db in PARAMETROS_RAPIDOS:
            if not hilo_lector_activo: break
            try:
                resp = connection.query(cmd)
                # Extraer magnitud (valor numérico) si la respuesta es válida
                val = resp.value.magnitude if not resp.is_null() else None
                with valores_lock:
                    valores_cache[etiqueta] = (val, unidad, db)
            except Exception:
                # Error en query → continuar con siguiente parámetro
                pass
                
        # ─────────────────────────────────────────────────────────────
        # LECTURA LENTA: Se ejecuta cada 1 segundo (throttled)
        # ─────────────────────────────────────────────────────────────
        ahora = time.time()
        if ahora - ultimo_lento >= MULTIPLEX_INTERVALO:
            for cmd, etiqueta, unidad, db in PARAMETROS_LENTOS:
                if not hilo_lector_activo: break
                try:
                    resp = connection.query(cmd)
                    val = resp.value.magnitude if not resp.is_null() else None
                    with valores_lock:
                        valores_cache[etiqueta] = (val, unidad, db)
                except Exception:
                    pass
            ultimo_lento = time.time()
            
        # Dormir 10ms para no saturar el adaptador OBD2
        time.sleep(0.01)


# ═════════════════════════════════════════════════════════════════════════════
# SISTEMA DE PUBLICACIÓN MQTT NO-BLOQUEANTE
# ═════════════════════════════════════════════════════════════════════════════
# Cola en memoria + hilo dedicado: desacopla la publicación MQTT del bucle principal.
# Así, si la red es lenta, el hilo OBD2 y la visualización NO se ven afectados.
#
cola_mqtt = queue.Queue(maxsize=MQTT_MAX_COLA)  # Cola: máx 50 mensajes pendientes
hilo_mqtt_activo = True                         # Flag: controla ejecución del hilo


def hilo_publicador_mqtt(cliente: mqtt.Client):
    """
    Hilo dedicado de publicación MQTT: desacopla red del loop principal.
    
    RESPONSABILIDAD ÚNICA:
      Consumir mensajes de la cola_mqtt y publicarlos en el broker MQTT.
    
    ARQUITECTURA NO-BLOQUEANTE:
      • Si el publish es lento (red congestionada), solo este hilo espera
      • El hilo OBD2 sigue leyendo sensores sin interrupciones
      • La interfaz Rich se sigue actualizando fluidamente
      • Si el broker no responde, otros hilos no se ven afectados
    
    CARACTERÍSTICA:
      • QoS 1: Espera ACK del broker (máximo 2s timeout)
      • RTT (Round-Trip Time) se registra en net_stats para diagnóstico
      • Si no hay mensaje en cola, espera 0.5s y reintenta
      • Continúa ejecutándose indefinidamente hasta que hilo_mqtt_activo sea False
    
    Args:
        cliente (mqtt.Client): Cliente MQTT ya conectado al broker
    
    Flujo:
      1. Intenta obtener mensaje de cola (timeout=0.5s)
      2. Si hay mensaje: mide tiempo, publica, actualiza RTT
      3. Si no hay: duerme y espera siguiente
      4. Actualiza contadores: enviados/drops/RTT en net_stats
    """
    while hilo_mqtt_activo:
        try:
            # Intenta obtener un mensaje de la cola (espera máximo 0.5s)
            payload_str = cola_mqtt.get(timeout=0.5)
        except queue.Empty:
            # No hay mensaje disponible aún → continuar esperando
            continue

        # ─────────────────────────────────────────────────────────────
        # PUBLICAR EN MQTT Y MEDIR LATENCIA
        # ─────────────────────────────────────────────────────────────
        t_pub = time.perf_counter()  # Marca de tiempo de inicio
        try:
            # Publicar mensaje en el topic MQTT
            info = cliente.publish(MQTT_TOPIC, payload_str, qos=MQTT_QOS)
            
            # QoS 1: esperar ACK del broker (máximo 2 segundos)
            if MQTT_QOS >= 1:
                info.wait_for_publish(timeout=2.0)
            
            # Calcular RTT en milisegundos
            rtt = (time.perf_counter() - t_pub) * 1000

            # Actualizar estadísticas
            with net_stats_lock:
                net_stats["enviados"] += 1
                net_stats["ultimo_rtt_ms"] = round(rtt, 1)
                net_stats["cola_actual"] = cola_mqtt.qsize()

        except Exception:
            # Error en publish → registrar drop
            with net_stats_lock:
                net_stats["drops"] += 1
                net_stats["cola_actual"] = cola_mqtt.qsize()


def encolar_mqtt(paquete: dict):
    """
    Añadir mensaje a la cola MQTT de forma segura, con backpressure.
    
    Si la cola está llena (>50 msgs), descarta el más antiguo para meter el nuevo.
    Esto implementa una política de "deslizante" para evitar bloqueos.
    
    ESTRATEGIA DE BACKPRESSURE:
      • Normal: put_nowait() → añade a la cola
      • Llena: get_nowait() → descarta el más viejo
      • Nuevamente llena: log drop y continúa
      • Si sigue llena: ignora (no causa crash)
    
    Args:
        paquete (dict): Diccionario con datos a publicar (se convierte a JSON)
    
    Efectos secundarios:
      • Actualiza net_stats["drops"] si hay descarte
      • Registra tamaño actual de cola
    """
    payload_str = json.dumps(paquete)
    try:
        cola_mqtt.put_nowait(payload_str)
    except queue.Full:
        # Cola llena → descartar el más viejo para meter el nuevo
        try:
            cola_mqtt.get_nowait()
        except queue.Empty:
            pass
        with net_stats_lock:
            net_stats["drops"] += 1
        try:
            cola_mqtt.put_nowait(payload_str)
        except queue.Full:
            # Si sigue llena después de descartar, renunciar silenciosamente
            pass


def leer_valores_cache() -> dict:
    """
    Leer de forma segura la caché de valores OBD2.
    
    Esta función crea una COPIA de la caché para evitar carreras de datos.
    El hilo OBD2 podría estar modificando valores_cache mientras se lee.
    
    Returns:
        dict: Copia segura de valores_cache
              {etiqueta → (valor, unidad, nombre_BD)}
    
    Ejemplo:
        >>> valores = leer_valores_cache()
        >>> print(valores["RPM"])
        (2500, "rpm", "rpm")
    """
    with valores_lock:
        return dict(valores_cache)


def construir_tabla(valores: dict) -> Table:
    """
    Construir tabla Rich para visualización en tiempo real de sensores.
    
    PROPÓSITO:
      Mostrar los valores actuales de OBD2 en una tabla bonita con estilos.
    
    CONTENIDO:
      • Columna 1: Nombre del parámetro (ej: "RPM", "Velocidad")
      • Columna 2: Valor actual con color (verde si disponible, rojo si N/D)
      • Columna 3: Unidad de medida (rpm, km/h, °C, %)
    
    RENDERIZADO:
      • Si valor es None o null → muestra "[red]N/D[/red]" (No Disponible)
      • Si valor válido → muestra número con 2 decimales en verde
      • Si número muy grande → trunca automáticamente
    
    Args:
        valores (dict): Caché de valores OBD2
                       {etiqueta → (valor, unidad, nombre_BD)}
    
    Returns:
        Table: Objeto tabla Rich lista para mostrar
    """
    tabla = Table(
        title="Adquisición Datos OBD2",
        box=box.ROUNDED,
        border_style="cyan"
    )
    tabla.add_column("Parámetro", style="bold white", min_width=20)
    tabla.add_column("Valor", justify="right", style="bold green", min_width=10)
    tabla.add_column("Unidad", style="dim", min_width=6)

    for cmd, etiqueta, unidad, db in PARAMETROS:
        tupla = valores.get(etiqueta)
        if tupla is not None and tupla[0] is not None:
            # Valor disponible: mostrar con 2 decimales en verde
            tabla.add_row(etiqueta, f"[green]{tupla[0]:.2f}[/green]", tupla[1])
        else:
            # Valor no disponible (None o sensensor sin respuesta)
            tabla.add_row(etiqueta, "[red]N/D[/red]", db)
    return tabla


def main():
    """
    FUNCIÓN PRINCIPAL: Orquestador de toda la captura de telemetría.
    
    FASES:
      1. Configurar archivo CSV para almacenar datos
      2. Configurar conexión MQTT al broker remoto
      3. Arrancar hilos independientes (OBD2, MQTT)
      4. Entrar en bucle de visualización en tiempo real
      5. Manejar interrupción (Ctrl+C) y limpieza
    
    FLUJO DE DATOS EN TIEMPO REAL:
      └─→ Hilo OBD2: Lee sensores del vehículo cada 10ms
          └─→ valores_cache (sincronizado con lock)
              ├─→ Hilo Principal: Lee caché y actualiza visualización Rich
              └─→ Hilo MQTT: Lee caché → cola_mqtt → publica por red
    
    CONTROL DE VELOCIDAD:
      • CSV: máx 10 Hz (1 fila cada 100ms) → Elimina duplicados
      • Rich UI: 4 FPS (actualiza cada 250ms)
      • OBD2: 100 Hz (rápidos) + 1 Hz (lentos)
    
    ESTADÍSTICAS MOSTRADAS:
      • RPM, Velocidad, Acelerador, Temperatura (valores actuales)
      • Estado: "Grabando" / "Duplicado" / "Throttle 10Hz" / "Esperando OBD"
      • MQTT: RTT (latencia), Cola, Drops, Mensajes enviados
      • Hz real: Frecuencia de actualización del bucle
      • Filas: Contador total de registros en CSV
    """
    global hilo_lector_activo, hilo_mqtt_activo
    console.rule("[bold cyan]Captura Datos OBD2[/bold cyan]")

    # ═════════════════════════════════════════════════════════════════════════════
    # FASE 1: CONFIGURAR CSV LOGGER
    # ═════════════════════════════════════════════════════════════════════════════
    fecha_actual = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = CARPETA_DATOS / f"dataset_skoda_{fecha_actual}.csv"
    archivo_csv = open(nombre_archivo, mode='w', newline='')
    writer = csv.writer(archivo_csv, delimiter=';')

    # Escribir cabeceras: timestamp + nombres de sensores
    cabeceras = ["timestamp", "timestamp_iso"] + [p[3] for p in PARAMETROS]
    writer.writerow(cabeceras)
    console.print(f"[bold blue]Dataset creado: {nombre_archivo}[/bold blue]")

    # ═════════════════════════════════════════════════════════════════════════════
    # FASE 2: CONFIGURAR MQTT
    # ═════════════════════════════════════════════════════════════════════════════
    mqtt_conectado = threading.Event()  # Event para señalizar si MQTT está conectado

    def on_connect(client, userdata, flags, reason_code, properties):
        """Callback: se ejecuta cuando el cliente se conecta al broker"""
        if reason_code == 0:
            mqtt_conectado.set()
            console.print("[bold green]MQTT conectado a Servidor.[/bold green]")
        else:
            console.print(f"[yellow]MQTT código {reason_code}[/yellow]")

    def on_disconnect(client, userdata, flags, reason_code, properties):
        """Callback: se ejecuta cuando se desconecta del broker"""
        mqtt_conectado.clear()
        console.print("[yellow]MQTT desconectado. Reconectando...[/yellow]")

    cliente_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    cliente_mqtt.on_connect = on_connect
    cliente_mqtt.on_disconnect = on_disconnect
    cliente_mqtt.reconnect_delay_set(min_delay=1, max_delay=30)

    try:
        cliente_mqtt.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        cliente_mqtt.loop_start()  # Inicia bucle MQTT en background
    except Exception as e:
        console.print(f"[yellow]MQTT error: {e}. Grabando solo en CSV.[/yellow]")

    # ═════════════════════════════════════════════════════════════════════════════
    # FASE 3: ARRANCAR HILOS INDEPENDIENTES
    # ═════════════════════════════════════════════════════════════════════════════
    # Hilo dedicado de publicación MQTT (desacoplado del loop principal)
    t_pub = threading.Thread(target=hilo_publicador_mqtt, args=(cliente_mqtt,), daemon=True)
    t_pub.start()

    # ═════════════════════════════════════════════════════════════════════════════
    # FASE 4: CONECTAR OBD2
    # ═════════════════════════════════════════════════════════════════════════════
    try:
        connection = conectar_obd_sync()
    except ConnectionError:
        console.print("[bold red]Cancelando captura.[/bold red]")
        return

    # Hilo de polling OBD2 (lectura continua de sensores)
    t_polling = threading.Thread(target=hilo_lector_obd, args=(connection,), daemon=True)
    t_polling.start()

    console.print("[dim]Llenando el buffer inicial...[/dim]")
    time.sleep(1.5)  # Esperar a que el caché se llene con valores reales

    console.print("\n[dim]Capturando datos... Pulsar Ctrl+C para finalizar.[/dim]\n")

    # ═════════════════════════════════════════════════════════════════════════════
    # VARIABLES DE CONTROL DEL BUCLE PRINCIPAL
    # ═════════════════════════════════════════════════════════════════════════════
    tiempos_ciclo = []              # Lista de tiempos de ciclo (últimos 50) para Hz promedio
    filas_sin_flush = 0             # Contador de filas antes de flush a disco
    ultimo_csv_ts = 0.0             # Timestamp del último write a CSV
    ultimos_valores_csv = None      # Tupla de valores para detectar duplicados
    filas_totales = 0               # Contador total de filas grabadas
    filas_duplicadas_omitidas = 0   # Estadística de duplicados descartados
    ultimo_dato_nuevo_ts = time.time()  # Timestamp del último dato nuevo (sin duplicados)

    try:
        # FASE 5: BUCLE PRINCIPAL DE CAPTURA Y VISUALIZACIÓN (4 FPS)
        with Live(console=console, refresh_per_second=RICH_REFRESH_HZ) as live:
            ultimo_loop_ts = time.time()
            while True:
                # Leer datos actuales del caché OBD2
                valores_actuales_dict = leer_valores_cache()
                tabla = construir_tabla(valores_actuales_dict)

                paquete_mqtt = {}       # Paquete a enviar por MQTT
                hay_datos_nuevos = False  # Flag para CSV + MQTT
                
                # Verificar si hay datos válidos (RPM como indicador)
                hay_datos_base = valores_actuales_dict.get("RPM", (None,))[0] is not None
                
                if hay_datos_base:
                    # ─ TIMESTAMP DE ORIGEN (para Telegraf + análisis)
                    ts_unix = time.time()               # Segundos desde epoch
                    ts_ns = time.time_ns()              # Nanosegundos (para Telegraf)
                    ts_iso = datetime.datetime.now().isoformat(timespec='milliseconds')  # ISO 8601
                    fila_csv = [round(ts_unix, 3), ts_iso]

                    # Inyectar timestamp en el paquete MQTT (en nanosegundos)
                    paquete_mqtt["timestamp"] = ts_ns

                    # ─ CONSTRUIR FILA CSV + PAQUETE MQTT
                    for cmd, etiqueta, unidad, db in PARAMETROS:
                        val = valores_actuales_dict[etiqueta][0]
                        if val is not None:
                            # Valor válido: redondear a 2 decimales
                            paquete_mqtt[db] = round(val, 2)
                            fila_csv.append(round(val, 2))
                            hay_datos_nuevos = True
                        else:
                            # Valor no disponible
                            fila_csv.append("")

                    if hay_datos_nuevos:
                        # ─ PUBLICAR MQTT DE FORMA NO-BLOQUEANTE
                        encolar_mqtt(paquete_mqtt)

                        # Leer métricas de red para el panel
                        with net_stats_lock:
                            rtt = net_stats["ultimo_rtt_ms"]
                            drops = net_stats["drops"]
                            q_size = net_stats["cola_actual"]
                            enviados = net_stats["enviados"]

                        if mqtt_conectado.is_set():
                            estado_mqtt = f"[green]MQTT | RTT:{rtt:.0f}ms Q:{q_size}[/green]"
                        else:
                            estado_mqtt = f"[yellow]MQTT | drops:{drops}[/yellow]"

                        ahora = time.time()
                        valores_plana = tuple(fila_csv[2:])  # Tupla de valores para comparación

                        # ─ ESCRITURA A CSV (THROTTLED A 10 Hz)
                        if (ahora - ultimo_csv_ts) >= MIN_INTERVALO_CSV:
                            if valores_plana != ultimos_valores_csv:
                                # DATO NUEVO: escribir a CSV
                                writer.writerow(fila_csv)
                                filas_sin_flush += 1
                                filas_totales += 1
                                ultimos_valores_csv = valores_plana
                                ultimo_csv_ts = ahora
                                ultimo_dato_nuevo_ts = ahora 
                                estado = "[bold green]Grabando[/bold green]"

                                # Flush a disco cada CSV_FLUSH_CADA filas
                                if filas_sin_flush >= CSV_FLUSH_CADA:
                                    archivo_csv.flush()
                                    filas_sin_flush = 0
                            else:
                                # DATO DUPLICADO: ignorar
                                filas_duplicadas_omitidas += 1
                                secs_sin_nuevo = ahora - ultimo_dato_nuevo_ts
                                if secs_sin_nuevo > 3:
                                    # Alerta: no hay cambios en 3+ segundos
                                    estado = f"[bold red]Sin cambios {secs_sin_nuevo:.0f}s[/bold red]"
                                else:
                                    # Duplicado normal (throttle de 10 Hz en acción)
                                    estado = "[bold blue]Duplicado (Ignorado)[/bold blue]"
                        else:
                            # Aún no pasó MIN_INTERVALO_CSV (throttle activo)
                            estado = "[bold cyan]Throttle 10Hz[/bold cyan]"
                else:
                    # Sin datos OBD2 (vehículo apagado o sin conexión)
                    estado = "[bold yellow]Esperando OBD...[/bold yellow]"
                    estado_mqtt = "-"

                # ─ CÁLCULO DE FRECUENCIA REAL DEL BUCLE
                t_ciclo = time.time() - ultimo_loop_ts
                ultimo_loop_ts = time.time()
                tiempos_ciclo.append(t_ciclo)
                if len(tiempos_ciclo) > 50: tiempos_ciclo.pop(0)
                hz_real = 1.0 / (sum(tiempos_ciclo) / len(tiempos_ciclo)) if tiempos_ciclo else 0

                # ─ ACTUALIZAR VISUALIZACIÓN RICH
                ts = time.strftime("%H:%M:%S")
                panel = Panel(
                    tabla,
                    subtitle=f"[dim]Hora: {ts} | {estado} | {estado_mqtt} | Hz: {hz_real:.1f} | Filas: {filas_totales}[/dim]",
                    border_style="cyan"
                )
                live.update(panel)

                # Dormir para mantener 4 FPS
                time.sleep(0.02)

    except KeyboardInterrupt:
        # MANEJO DE INTERRUPCIÓN (Ctrl+C): LIMPIEZA Y CIERRE
        hilo_lector_activo = False
        hilo_mqtt_activo = False
        console.print("\n[bold yellow] Detenido. [/bold yellow]")
        
        # Mostrar estadísticas finales
        with net_stats_lock:
            console.print(f"[dim]MQTT stats: enviados={net_stats['enviados']}, drops={net_stats['drops']}, último RTT={net_stats['ultimo_rtt_ms']}ms[/dim]")
        if tiempos_ciclo:
            hz_promedio = 1.0 / (sum(tiempos_ciclo) / len(tiempos_ciclo))
            console.print(f"[dim]Bucle cerrado a {hz_promedio:.1f} Hz[/dim]")
        
    finally:
        # CIERRE DE RECURSOS (SIEMPRE SE EJECUTA)
        hilo_lector_activo = False
        hilo_mqtt_activo = False
        time.sleep(0.5)  # Esperar a que los hilos terminen
        
        connection.close()  # Cerrar conexión OBD2
        archivo_csv.flush()  # Flush final de datos pendientes
        archivo_csv.close()  # Cerrar archivo CSV
        cliente_mqtt.disconnect()  # Desconectar de MQTT
        
        console.print(f"[bold green]Dataset guardado: {nombre_archivo}[/bold green]")


if __name__ == "__main__":
    """
    PUNTO DE ENTRADA DEL PROGRAMA
    
    Ejecuta la función main() que inicia toda la captura de telemetría.
    Los datos se almacenan en CSV y se transmiten por MQTT.
    """
    main()