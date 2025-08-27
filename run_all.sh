#!/bin/bash

echo "🚀 Iniciando TFG completo..."

# Activar entorno virtual
source venv/bin/activate

# Iniciar contenedor RabbitMQ
echo "🐳 Iniciando RabbitMQ..."
cd docker
docker-compose up -d
cd ..

# Ejecutar consumidor
echo "📦 Ejecutando consumidor (Spark)..."
#gnome-terminal -- bash -c "cd backend && python consumidor.py; exec bash"
python backend/consumidor.py
# Ejecutar productor
echo "📦 Ejecutando productor..."
python backend/productor.py

# Ejecutar interfaz Flask
echo "🌐 Lanzando interfaz web Flask..."
python frontend/app.py
cd ..

echo "✅ Todo iniciado. Abre http://localhost:5000 en tu navegador."
