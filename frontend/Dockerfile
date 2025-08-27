# Imagen base con Python
FROM python:3.9-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia todos los archivos del proyecto al contenedor
COPY . /app

# Instala las dependencias necesarias
RUN pip install --no-cache-dir flask pandas pika

# Expone el puerto 5000 (donde corre Flask)
EXPOSE 5000

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]