# 🧠 TFG - Sistema Distribuido con RabbitMQ, Apache Spark y Frontend Web

**Autor**: Diego Villena Macarrón  
**Grado**: Ingeniería Informática  
**Universidad**: Universidad Alfonso X el Sabio (UAX)  
**Curso**: 4º - 2024/2025

---

## 📌 Descripción

Este proyecto de Trabajo Fin de Grado consiste en el diseño e implementación de un **sistema distribuido de procesamiento de pedidos**, que combina:

- **RabbitMQ** como sistema de mensajería distribuido
- **Apache Spark** para el procesamiento en lote
- **Flask** como frontend web para visualizar los pedidos procesados

El sistema simula pedidos enviados desde almacenes, que son procesados por Spark y almacenados en CSV para ser visualizados desde una interfaz web.

---

## 🧱 Arquitectura del sistema

```plaintext
[ Productor Python ] --> (RabbitMQ) --> [ Spark + Consumidor ] --> [ CSV ] --> [ Flask Web ]
