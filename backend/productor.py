#!/usr/bin/env python3
# productor.py
# 6 productores enviando 5 pedidos cada uno a colas aleatorias

import pika, json, time, random, threading
from faker import Faker
from datetime import datetime
from faker import Faker

fake = Faker()

continentes = ["Asia", "America", "Europa"]
continente_to_port = {
    "Asia": 5672,
    "America": 5673,
    "Europa": 5674
}

# Contador global para estadísticas
pedidos_por_continente = {"Asia": 0, "America": 0, "Europa": 0}
pedidos_por_productor = {f"Productor_{i+1}": {"Asia": 0, "America": 0, "Europa": 0} for i in range(6)}
todos_los_pedidos = []  # Lista para guardar todos los pedidos generados
lock = threading.Lock()

def send_to_continent(pedido, continent, producer_id):
    port = continente_to_port[continent]
    creds = pika.PlainCredentials('guest', 'guest')
    params = pika.ConnectionParameters('localhost', port=port, credentials=creds)
    
    try:
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.queue_declare(queue='pedidos', durable=True)
        ch.basic_publish(exchange='', routing_key='pedidos', body=json.dumps(pedido),
                         properties=pika.BasicProperties(delivery_mode=2))
        conn.close()
        
        # Actualizar contadores
        with lock:
            pedidos_por_continente[continent] += 1
            pedidos_por_productor[producer_id][continent] += 1
            todos_los_pedidos.append(pedido)
            
        print(f"[{producer_id}] Enviado pedido {pedido['id']} -> {continent} (Producto: {pedido['producto']}, Cliente: {pedido['cliente']})")
        return True
    except Exception as e:
        print(f"[{producer_id}] Error al enviar a {continent}: {e}")
        return False

def generate_order(i, producer_id):
    productos = ['Monitor', 'Teclado', 'Ratón', 'Portátil', 'Router', 'Impresora', 'Webcam', 'Altavoces', 'SSD', 'Memoria RAM']
    almacenes = ['Madrid', 'Sevilla', 'Barcelona', 'Valencia', 'Bilbao', 'Zaragoza']
    precios_base = {'Monitor': 200, 'Teclado': 50, 'Ratón': 30, 'Portátil': 800, 'Router': 100, 'Impresora': 150, 'Webcam': 80, 'Altavoces': 60, 'SSD': 120, 'Memoria RAM': 90}
    
    # Seleccionar continente aleatorio
    continent = random.choice(continentes)
    producto = random.choice(productos)
    cantidad = random.randint(1, 15)
    precio_unitario = precios_base.get(producto, 100)
    precio_total = precio_unitario * cantidad
    
    pedido = {
        "id": f"{producer_id}_{int(time.time()*1000)}_{i}",
        "productor": producer_id,
        "almacen": random.choice(almacenes),
        "producto": producto,
        "cantidad": cantidad,
        "precio_unitario": precio_unitario,
        "precio_total": precio_total,
        "cliente": fake.name(),
        "direccion": fake.address().replace("\n", ", "),
        "telefono": fake.phone_number(),
        "email": fake.email(),
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "continente": continent,
        "estado": "pendiente"
    }
    return pedido, continent

def producer_worker(producer_id):
    print(f"[{producer_id}] Iniciando - enviará 5 pedidos")
    
    for i in range(5):
        pedido, continent = generate_order(i, producer_id)
        if send_to_continent(pedido, continent, producer_id):
            print(f"[{producer_id}] ✅ Pedido {i+1}/5 enviado exitosamente")
        else:
            print(f"[{producer_id}] ❌ Error enviando pedido {i+1}/5")
        time.sleep(random.uniform(0.5, 1.5))  # Pausa aleatoria entre pedidos
    
    print(f"[{producer_id}] ✅ Terminado - 5 pedidos enviados")

def guardar_estadisticas():
    """Guarda las estadísticas en un archivo JSON"""
    import os
    os.makedirs("../datos/stats", exist_ok=True)
    
    stats = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pedidos_por_continente": pedidos_por_continente,
        "pedidos_por_productor": pedidos_por_productor,
        "total_pedidos": sum(pedidos_por_continente.values()),
        "todos_los_pedidos": todos_los_pedidos
    }
    
    filename = f"../datos/stats/produccion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    print(f"📊 Estadísticas guardadas en: {filename}")

def main():
    print("=== INICIANDO 6 PRODUCTORES ===")
    print("Cada productor enviará 5 pedidos a continentes aleatorios")
    print("Continentes disponibles:", continentes)
    print("=" * 50)
    
    start_time = time.time()
    
    # Crear threads para los 6 productores
    threads = []
    for i in range(6):
        producer_id = f"Productor_{i+1}"
        thread = threading.Thread(target=producer_worker, args=(producer_id,))
        threads.append(thread)
        thread.start()
        time.sleep(0.2)  # Pequeña pausa entre inicios
    
    # Esperar a que terminen todos
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    
    # Mostrar estadísticas finales
    print("\n" + "=" * 60)
    print("🎯 ESTADÍSTICAS FINALES DE PRODUCCIÓN")
    print("=" * 60)
    
    print("\n📊 Pedidos por continente:")
    for cont, count in pedidos_por_continente.items():
        porcentaje = (count / sum(pedidos_por_continente.values())) * 100 if sum(pedidos_por_continente.values()) > 0 else 0
        print(f"  {cont:8}: {count:2} pedidos ({porcentaje:.1f}%)")
    
    print("\n👥 Pedidos por productor y continente:")
    print("      Productor    |  Asia  | América | Europa | Total")
    print("      -------------|--------|---------|--------|-------")
    for prod_id, stats in pedidos_por_productor.items():
        total = sum(stats.values())
        print(f"      {prod_id:12} | {stats['Asia']:6} | {stats['America']:7} | {stats['Europa']:6} | {total:5}")
    
    total_global = sum(pedidos_por_continente.values())
    print(f"\n🎯 TOTAL GLOBAL: {total_global} pedidos enviados")
    print(f"⏱️  Tiempo de ejecución: {execution_time} segundos")
    print(f"📈 Velocidad promedio: {total_global/execution_time:.2f} pedidos/segundo")
    
    # Mostrar algunos pedidos de ejemplo
    print(f"\n📦 Últimos 3 pedidos generados:")
    for pedido in todos_los_pedidos[-3:]:
        print(f"   • {pedido['id']} - {pedido['producto']} x{pedido['cantidad']} → {pedido['continente']} (€{pedido['precio_total']})")
    
    # Guardar estadísticas
    guardar_estadisticas()
    
    print("\n✅ PRODUCCIÓN COMPLETADA")
    print("=" * 60)

if __name__ == "__main__":
    main()