#!/usr/bin/env python3
# consumidor.py
# Consumidor especializado por continente
# Uso: python consumidor.py --continent Asia

import pika, json, os, csv, argparse, time
from datetime import datetime

continente_to_port = {
    "Asia": 5672,
    "America": 5673,
    "Europa": 5674
}

class ConsumidorContinente:
    def __init__(self, continent):
        self.continent = continent
        self.port = continente_to_port[continent]
        self.pedidos_procesados = []
        self.batch = []
        self.BATCH_SIZE = 5
        self.total_procesados = 0
        
    def conectar_rabbitmq(self):
        """Establece conexión con RabbitMQ"""
        creds = pika.PlainCredentials('guest', 'guest')
        params = pika.ConnectionParameters('localhost', port=self.port, credentials=creds)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='pedidos', durable=True)
        print(f"[Consumer-{self.continent}] ✅ Conectado a puerto {self.port}")

    def guardar_batch(self, pedidos):
        """Guarda un lote de pedidos en CSV"""
        if not pedidos:
            return
            
        os.makedirs(os.path.join("..","datos","pedidos"), exist_ok=True)
        fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join("..","datos","pedidos", f"{self.continent}_pedidos_{fecha}.csv")
        
        # Definir todas las columnas posibles
        fieldnames = [
            'ID Pedido', 'Productor', 'Almacén', 'Producto', 'Cantidad', 
            'Precio Unitario', 'Precio Total', 'Cliente', 'Dirección', 
            'Teléfono', 'Email', 'Fecha', 'Continente', 'Estado', 
            'Fecha Procesado'
        ]
        
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(pedidos)
        
        print(f"[Consumer-{self.continent}] 💾 Guardados {len(pedidos)} pedidos en {path}")
        
        # Log en archivo de registro general
        self.escribir_log(f"Procesados {len(pedidos)} pedidos - Total acumulado: {self.total_procesados}")

    def escribir_log(self, mensaje):
        """Escribe en el log general del sistema"""
        log_path = os.path.join("..","datos","logs","registro.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] Consumer-{self.continent}: {mensaje}\n")

    def procesar_pedido(self, pedido_raw):
        """Procesa un pedido individual y lo añade al batch"""
        try:
            pedido = json.loads(pedido_raw.decode())
            
            # Enriquecer el pedido con información de procesamiento
            pedido_procesado = {
                'ID Pedido': pedido.get('id', 'N/A'),
                'Productor': pedido.get('productor', 'N/A'),
                'Almacén': pedido.get('almacen', 'N/A'),
                'Producto': pedido.get('producto', 'N/A'),
                'Cantidad': pedido.get('cantidad', 0),
                'Precio Unitario': pedido.get('precio_unitario', 0),
                'Precio Total': pedido.get('precio_total', 0),
                'Cliente': pedido.get('cliente', 'N/A'),
                'Dirección': pedido.get('direccion', 'N/A'),
                'Teléfono': pedido.get('telefono', 'N/A'),
                'Email': pedido.get('email', 'N/A'),
                'Fecha': pedido.get('fecha', 'N/A'),
                'Continente': pedido.get('continente', self.continent),
                'Estado': 'procesado',
                'Fecha Procesado': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            self.batch.append(pedido_procesado)
            self.total_procesados += 1
            
            print(f"[Consumer-{self.continent}] 📦 Procesado: {pedido.get('id')} de {pedido.get('productor', 'Unknown')} - Producto: {pedido.get('producto', 'N/A')} (€{pedido.get('precio_total', 0)})")
            
            # Si el batch está completo, guardarlo
            if len(self.batch) >= self.BATCH_SIZE:
                self.guardar_batch(self.batch)
                self.batch = []
                
        except Exception as e:
            print(f"[Consumer-{self.continent}] ❌ Error procesando pedido: {e}")

    def callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de RabbitMQ"""
        self.procesar_pedido(body)
        # Simular tiempo de procesamiento
        time.sleep(0.1)

    def iniciar_consumo(self):
        """Inicia el consumo de mensajes"""
        print(f"[Consumer-{self.continent}] 🚀 Iniciando consumo en continente {self.continent}")
        print(f"[Consumer-{self.continent}] 📡 Escuchando en puerto {self.port}...")
        
        self.escribir_log(f"Iniciado consumidor para {self.continent}")
        
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue='pedidos', 
            on_message_callback=self.callback, 
            auto_ack=True
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(f"\n[Consumer-{self.continent}] 🛑 Interrumpido por usuario")
        except Exception as e:
            print(f"[Consumer-{self.continent}] ❌ Excepción: {e}")
        finally:
            self.finalizar()

    def finalizar(self):
        """Finaliza el consumidor y guarda datos pendientes"""
        # Guardar batch pendiente si existe
        if self.batch:
            print(f"[Consumer-{self.continent}] 💾 Guardando {len(self.batch)} pedidos pendientes...")
            self.guardar_batch(self.batch)
        
        # Cerrar conexión
        try:
            if hasattr(self, 'connection') and not self.connection.is_closed:
                self.connection.close()
        except:
            pass
        
        print(f"[Consumer-{self.continent}] ✅ Finalizado - Total procesados: {self.total_procesados} pedidos")
        self.escribir_log(f"Finalizado - Total procesados: {self.total_procesados} pedidos")

def main():
    parser = argparse.ArgumentParser(
        description="Consumidor de pedidos por continente",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python consumidor.py --continent Asia
  python consumidor.py --continent America  
  python consumidor.py --continent Europa
        """
    )
    parser.add_argument(
        "--continent", 
        required=True, 
        choices=["Asia","America","Europa"],
        help="Continente a procesar (Asia, America, Europa)"
    )
    
    args = parser.parse_args()
    
    print("=" * 50)
    print(f"🌍 CONSUMIDOR DE PEDIDOS - {args.continent.upper()}")
    print("=" * 50)
    
    consumidor = ConsumidorContinente(args.continent)
    
    try:
        consumidor.conectar_rabbitmq()
        consumidor.iniciar_consumo()
    except Exception as e:
        print(f"❌ Error crítico: {e}")
    finally:
        consumidor.finalizar()

if __name__ == "__main__":
    main()