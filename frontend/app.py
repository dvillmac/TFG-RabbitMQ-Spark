#!/usr/bin/env python3
from flask import Flask, render_template, redirect, url_for, request, send_from_directory, flash, jsonify
import os, subprocess, csv, time, json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

app = Flask(__name__)
app.secret_key = "tfg-secret-key-2024"

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RUNTIME = os.path.join(BASE, 'runtime')
os.makedirs(RUNTIME, exist_ok=True)

CONTINENTS = ["Asia","America","Europa"]
RABBITMQ_MANAGEMENT_PORT = {
    "Asia": 15672,
    "America": 15673,
    "Europa": 15674
}
RABBITMQ_AMQP_PORT = {
    "Asia": 5672,
    "America": 5673,
    "Europa": 5674
}

def pidfile(name):
    return os.path.join(RUNTIME, f"{name}.pid")

def is_process_running(name):
    """Verifica si un proceso está ejecutándose"""
    pfile = pidfile(name)
    if not os.path.exists(pfile):
        return False
    
    try:
        with open(pfile) as f:
            pid = int(f.read().strip())
        # Verificar si el proceso realmente existe
        os.kill(pid, 0)  # No mata el proceso, solo verifica
        return True
    except (OSError, ValueError):
        # El proceso no existe, limpiar el pidfile
        try:
            os.remove(pfile)
        except:
            pass
        return False

def start_process(name, cmd, cwd=None):
    pfile = pidfile(name)
    if is_process_running(name):
        return False, f"{name} ya está en ejecución"
    
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(pfile, 'w') as f:
            f.write(str(p.pid))
        return True, f"{name} iniciado (PID {p.pid})"
    except Exception as e:
        return False, f"Error iniciando {name}: {e}"

def stop_process(name):
    pfile = pidfile(name)
    if not is_process_running(name):
        return False, f"{name} no se está ejecutando"
    
    try:
        with open(pfile) as f:
            pid = int(f.read().strip())
        os.kill(pid, 9)
        os.remove(pfile)
        return True, f"{name} detenido"
    except Exception as e:
        return False, f"Error deteniendo {name}: {e}"

def get_queue_info(continent):
    """Obtener información detallada de la cola RabbitMQ"""
    port = RABBITMQ_MANAGEMENT_PORT[continent]
    try:
        r = requests.get(f'http://localhost:{port}/api/queues/%2F/pedidos', 
                        auth=HTTPBasicAuth('guest','guest'), timeout=3)
        if r.status_code == 200:
            data = r.json()
            return {
                'name': data.get('name', 'pedidos'),
                'messages': data.get('messages', 0),
                'messages_ready': data.get('messages_ready', 0),
                'messages_unacknowledged': data.get('messages_unacknowledged', 0),
                'consumers': data.get('consumers', 0),
                'message_stats': data.get('message_stats', {}),
                'status': 'online'
            }
    except Exception as e:
        print(f"Error obteniendo info de cola {continent}: {e}")
    
    return {
        'name': 'pedidos', 
        'messages': 0, 
        'messages_ready': 0,
        'messages_unacknowledged': 0,
        'consumers': 0,
        'message_stats': {},
        'status': 'offline'
    }

def cargar_estadisticas_produccion():
    """Carga las últimas estadísticas de producción"""
    stats_dir = os.path.join(BASE, 'datos', 'stats')
    if not os.path.exists(stats_dir):
        return None
    
    # Buscar el archivo más reciente
    archivos = [f for f in os.listdir(stats_dir) if f.startswith('produccion_') and f.endswith('.json')]
    if not archivos:
        return None
    
    archivo_reciente = sorted(archivos)[-1]
    try:
        with open(os.path.join(stats_dir, archivo_reciente), 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error cargando estadísticas: {e}")
        return None

def obtener_pedidos_procesados():
    """Obtiene todos los pedidos procesados de los CSVs"""
    pedidos_dir = os.path.join(BASE, 'datos', 'pedidos')
    pedidos = []
    
    if os.path.exists(pedidos_dir):
        for fname in sorted(os.listdir(pedidos_dir), reverse=True):
            if fname.endswith('.csv'):
                path = os.path.join(pedidos_dir, fname)
                try:
                    with open(path, newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            pedidos.append(row)
                except Exception as e:
                    print(f"Error leyendo {path}: {e}")
    
    return pedidos

@app.route('/')
def index():
    return redirect(url_for('dashboard'))

@app.route('/dashboard')
def dashboard():
    # Cargar estadísticas de producción
    stats_produccion = cargar_estadisticas_produccion()
    
    # Obtener pedidos procesados
    pedidos_procesados = obtener_pedidos_procesados()
    
    # Contar totales
    total_pedidos = len(pedidos_procesados)
    region_count = {"Asia": 0, "America": 0, "Europa": 0}
    
    # Estadísticas por productor (inicializar desde stats de producción si existe)
    if stats_produccion:
        producer_stats = stats_produccion.get('pedidos_por_productor', {})
    else:
        producer_stats = {f"Productor_{i+1}": {"Asia": 0, "America": 0, "Europa": 0} for i in range(6)}
    
    # Contar pedidos procesados por región
    for pedido in pedidos_procesados:
        continent = pedido.get('Continente', '')
        if continent in region_count:
            region_count[continent] += 1

    # Estado RabbitMQ y información de colas
    rabbit_status = {}
    queue_info = {}
    for c in CONTINENTS:
        port = RABBITMQ_MANAGEMENT_PORT[c]
        try:
            r = requests.get(f'http://localhost:{port}/api/healthchecks/node', 
                           auth=HTTPBasicAuth('guest','guest'), timeout=2)
            rabbit_status[c] = (r.status_code == 200)
        except:
            rabbit_status[c] = False
        
        queue_info[c] = get_queue_info(c)

    # Estados de procesos
    process_status = {}
    for c in CONTINENTS:
        process_status[f"consumer_{c}"] = is_process_running(f"consumer_{c}")
    process_status["producers"] = is_process_running("producers")

    # Docker containers
    try:
        out = subprocess.check_output(['docker','ps','--format','{{.Names}} - {{.Status}}']).decode().splitlines()
    except Exception as e:
        out = [f"Error: {e}"]

    # Logs recientes
    logpath = os.path.join(BASE, 'datos', 'logs', 'registro.log')
    logs = []
    if os.path.exists(logpath):
        try:
            with open(logpath,'r',encoding='utf-8') as f:
                logs = f.readlines()[-50:]  # Últimas 50 líneas
        except Exception as e:
            logs = [f"Error leyendo logs: {e}"]

    # Archivos CSV generados
    csv_files = []
    pedidos_dir = os.path.join(BASE, 'datos', 'pedidos')
    if os.path.exists(pedidos_dir):
        for fname in os.listdir(pedidos_dir):
            if fname.endswith('.csv'):
                path = os.path.join(pedidos_dir, fname)
                try:
                    size_kb = round(os.path.getsize(path) / 1024, 1)
                    csv_files.append({'name': fname, 'size': size_kb})
                except:
                    csv_files.append({'name': fname, 'size': 0})

    return render_template('dashboard.html',
                           total_pedidos=total_pedidos,
                           region_count=region_count,
                           producer_stats=producer_stats,
                           latest_orders=pedidos_procesados[:20],  # Últimos 20
                           rabbit_status=rabbit_status,
                           queue_info=queue_info,
                           process_status=process_status,
                           containers=out,
                           logs=logs,
                           csv_files=csv_files,
                           stats_produccion=stats_produccion,
                           continents=CONTINENTS)

@app.route('/start-all-producers')
def start_all_producers():
    """Iniciar los 6 productores"""
    name = "producers"
    cmd = ["python3", os.path.join(BASE, "backend", "productor.py")]
    ok, msg = start_process(name, cmd, cwd=os.path.join(BASE, 'backend'))
    flash(msg, 'success' if ok else 'error')
    return redirect(url_for('dashboard'))

@app.route('/stop-all-producers')
def stop_all_producers():
    """Detener todos los productores"""
    name = "producers"
    ok, msg = stop_process(name)
    flash(msg, 'success' if ok else 'error')
    return redirect(url_for('dashboard'))

@app.route('/start-consumer')
def start_consumer_route():
    continent = request.args.get('continent')
    if continent not in CONTINENTS:
        flash("Continente no válido", 'error')
        return redirect(url_for('dashboard'))
    
    name = f"consumer_{continent}"
    cmd = ["python3", os.path.join(BASE, "backend", "consumidor.py"), "--continent", continent]
    ok, msg = start_process(name, cmd, cwd=os.path.join(BASE, 'backend'))
    flash(msg, 'success' if ok else 'error')
    return redirect(url_for('dashboard'))

@app.route('/stop-consumer')
def stop_consumer_route():
    continent = request.args.get('continent')
    name = f"consumer_{continent}"
    ok, msg = stop_process(name)
    flash(msg, 'success' if ok else 'error')
    return redirect(url_for('dashboard'))

@app.route('/view-details')
def view_details():
    """Vista detallada con todos los pedidos procesados"""
    pedidos = obtener_pedidos_procesados()
    stats_produccion = cargar_estadisticas_produccion()
    
    return render_template('details.html', 
                         orders=pedidos, 
                         stats_produccion=stats_produccion)

@app.route('/view-queue')
def view_queue():
    """Ver estado detallado de una cola específica"""
    continent = request.args.get('continent', 'Asia')
    if continent not in CONTINENTS:
        continent = 'Asia'
    
    queue_info = get_queue_info(continent)
    
    # Obtener mensajes recientes (si RabbitMQ management lo permite)
    recent_messages = []
    port = RABBITMQ_MANAGEMENT_PORT[continent]
    
    try:
        # Intentar obtener algunos mensajes de la cola
        r = requests.get(f'http://localhost:{port}/api/queues/%2F/pedidos/get', 
                        auth=HTTPBasicAuth('guest','guest'), 
                        json={"count":10,"ackmode":"ack_requeue_false","encoding":"auto"},
                        timeout=2)
        if r.status_code == 200:
            messages = r.json()
            for msg in messages:
                if 'payload' in msg:
                    try:
                        payload = json.loads(msg['payload'])
                        recent_messages.append(payload)
                    except:
                        pass
    except Exception as e:
        print(f"Error obteniendo mensajes de cola: {e}")
    
    return render_template('queue.html', 
                         continent=continent,
                         queue_info=queue_info,
                         messages=recent_messages,
                         continents=CONTINENTS)

@app.route('/reset-stats')
def reset_stats():
    """Resetear estadísticas del sistema"""
    try:
        # Limpiar directorio de pedidos
        pedidos_dir = os.path.join(BASE, 'datos', 'pedidos')
        if os.path.exists(pedidos_dir):
            for file in os.listdir(pedidos_dir):
                os.remove(os.path.join(pedidos_dir, file))
        
        # Limpiar estadísticas
        stats_dir = os.path.join(BASE, 'datos', 'stats')
        if os.path.exists(stats_dir):
            for file in os.listdir(stats_dir):
                os.remove(os.path.join(stats_dir, file))
        
        # Limpiar logs
        log_file = os.path.join(BASE, 'datos', 'logs', 'registro.log')
        if os.path.exists(log_file):
            open(log_file, 'w').close()
        
        flash("Estadísticas reseteadas correctamente", 'success')
    except Exception as e:
        flash(f"Error reseteando estadísticas: {e}", 'error')
    
    return redirect(url_for('dashboard'))

@app.route('/restart-docker')
def restart_docker():
    try:
        subprocess.check_call(['docker-compose','-f', os.path.join(BASE,'docker','docker-compose.yml'),'down'])
        time.sleep(2)
        subprocess.check_call(['docker-compose','-f', os.path.join(BASE,'docker','docker-compose.yml'),'up','-d'])
        flash("Docker reiniciado correctamente", 'success')
    except Exception as e:
        flash(f"Error al reiniciar Docker: {e}", 'error')
    return redirect(url_for('dashboard'))

@app.route('/api/queue_stats')
def api_queue_stats():
    """API para obtener estadísticas en tiempo real de todas las colas"""
    stats = {}
    for c in CONTINENTS:
        stats[c] = get_queue_info(c)
    
    return jsonify(stats)

@app.route('/api/system_status')
def api_system_status():
    """API completa del estado del sistema"""
    pedidos_procesados = obtener_pedidos_procesados()
    stats_produccion = cargar_estadisticas_produccion()
    
    # Contar por región
    region_count = {"Asia": 0, "America": 0, "Europa": 0}
    for pedido in pedidos_procesados:
        continent = pedido.get('Continente', '')
        if continent in region_count:
            region_count[continent] += 1
    
    # Estado de procesos
    process_status = {}
    for c in CONTINENTS:
        process_status[f"consumer_{c}"] = is_process_running(f"consumer_{c}")
    process_status["producers"] = is_process_running("producers")
    
    # Estado RabbitMQ
    rabbit_status = {}
    queue_info = {}
    for c in CONTINENTS:
        try:
            r = requests.get(f'http://localhost:{RABBITMQ_MANAGEMENT_PORT[c]}/api/healthchecks/node', 
                           auth=HTTPBasicAuth('guest','guest'), timeout=1)
            rabbit_status[c] = (r.status_code == 200)
        except:
            rabbit_status[c] = False
        queue_info[c] = get_queue_info(c)
    
    return jsonify({
        'timestamp': datetime.now().isoformat(),
        'total_pedidos': len(pedidos_procesados),
        'region_count': region_count,
        'process_status': process_status,
        'rabbit_status': rabbit_status,
        'queue_info': queue_info,
        'stats_produccion': stats_produccion
    })

@app.route('/csv-download/<filename>')
def csv_download(filename):
    """Descargar archivo CSV"""
    pedidos_dir = os.path.join(BASE, 'datos', 'pedidos')
    return send_from_directory(pedidos_dir, filename, as_attachment=True)

if __name__ == "__main__":
    # Crear directorios necesarios
    os.makedirs(os.path.join(BASE, 'datos', 'pedidos'), exist_ok=True)
    os.makedirs(os.path.join(BASE, 'datos', 'logs'), exist_ok=True)
    os.makedirs(os.path.join(BASE, 'datos', 'stats'), exist_ok=True)
    
    print("🚀 Iniciando TFG Dashboard...")
    print("🌐 Accede a http://localhost:5000")
    print("📊 Dashboard completo con estadísticas en tiempo real")
    
    app.run(host="0.0.0.0", port=5000, debug=True)