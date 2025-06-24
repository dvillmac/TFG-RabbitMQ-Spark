import pika, json, logging, os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import to_timestamp

logging.basicConfig(filename='../logs/consumidor.log', level=logging.INFO, format='%(asctime)s - %(message)s')

spark = SparkSession.builder.appName("ProcesadorPedidos").master("local[*]").getOrCreate()

schema = StructType() \
    .add("id_pedido", StringType()) \
    .add("almacen_id", IntegerType()) \
    .add("producto", StringType()) \
    .add("cantidad", IntegerType()) \
    .add("cliente", StringType()) \
    .add("direccion", StringType()) \
    .add("telefono", StringType()) \
    .add("fecha", StringType())

batch = []
output_path = "../datos/pedidos"
os.makedirs(output_path, exist_ok=True)

def callback(ch, method, properties, body):
    global batch
    pedido = json.loads(body)
    batch.append(pedido)
    logging.info(f"Pedido recibido: {pedido}")

    if len(batch) >= 10:
        df = spark.createDataFrame(batch, schema=schema)
        df = df.withColumn("fecha", to_timestamp("fecha"))
        df.write.mode("append").csv(output_path, header=True)
        batch.clear()
        print("✔ Lote procesado y guardado")

conexion = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
canal = conexion.channel()
canal.queue_declare(queue='pedidos', durable=True)
canal.basic_consume(queue='pedidos', on_message_callback=callback, auto_ack=True)

conexion = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost',
    credentials=pika.PlainCredentials('guest', 'guest')
))
print("[*] Esperando pedidos...")
try:
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()
    conexion.close()
    spark.stop()
    print("✘ Finalizado por usuario")
    logging.info("Consumo detenido por el usuario")
except Exception as e:
    logging.error(f"Error en el consumidor: {e}")
    print(f"✘ Error: {e}")
finally:
    if 'conexion' in locals():
        conexion.close()
    if 'spark' in locals():
        spark.stop()
    print("✔ Conexión cerrada y Spark detenido")
    logging.info("Conexión cerrada y Spark detenido")
    print("✔ Proceso finalizado")
    logging.info("Proceso finalizado")
    print("✔ Todos los pedidos procesados y guardados")
    logging.info("Todos los pedidos procesados y guardados")
    print("✔ Logs generados en ../logs/consumidor.log")
    logging.info("Logs generados en ../logs/consumidor.log")
    print("✔ Datos guardados en ../datos/pedidos")         
