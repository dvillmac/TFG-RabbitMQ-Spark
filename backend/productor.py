import pika, json, random, time, logging
from datetime import datetime
from faker import Faker

fake = Faker()
logging.basicConfig(filename='../logs/productor.log', level=logging.INFO, format='%(asctime)s - %(message)s')

productos = ["ratón", "teclado", "monitor", "portátil", "impresora"]
almacenes = [1001, 1002, 1003, 1004]

conexion = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
canal = conexion.channel()
conexion = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost',
    credentials=pika.PlainCredentials('guest', 'guest')
))
canal.queue_declare(queue='pedidos', durable=True)

for i in range(100):
    pedido = {
        "id_pedido": f"PED-{int(time.time())}-{i}",
        "almacen_id": random.choice(almacenes),
        "producto": random.choice(productos),
        "cantidad": random.randint(1, 200),
        "cliente": fake.name(),
        "direccion": fake.address(),
        "telefono": fake.phone_number(),
        "fecha": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    canal.basic_publish(
        exchange='',
        routing_key='pedidos',
        body=json.dumps(pedido),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    logging.info(f"Pedido enviado: {pedido}")
    time.sleep(random.uniform(0.5, 1.5))

conexion.close()
print("✔ Todos los pedidos enviados")
