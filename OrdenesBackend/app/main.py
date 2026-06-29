import json
from fastapi import FastAPI, HTTPException
from app.schemas import OrderRequest
from confluent_kafka import Producer

app = FastAPI()

# 1. Configuración del Productor de Kafka
# Apunta al nombre del puerto expuesto en tu localhost de Docker
kafka_config = {
    'bootstrap.servers': '192.168.1.49:9092',
    'client.id': 'modulo-procesamiento-ordenes'
}

producer = Producer(kafka_config)

# Función opcional de callback para verificar si el mensaje llegó al broker con éxito
def delivery_report(err, msg):
    if err is not None:
        print(Traceback / f"Error al entregar mensaje: {err}")
    else:
        print(f"Mensaje entregado con éxito a {msg.topic()} [Partición: {msg.partition()}]")

@app.post("/orders")
def create_order(order: OrderRequest):

    # Validaciones existentes
    if not order.items:
        raise HTTPException(
            status_code=400,
            detail={
                "status": "ERROR",
                "message": "Hubo un error al enviar su orden"
            }
        )

    for item in order.items:
        if item.quantity <= 0:
            raise HTTPException(
                status_code=422,
                detail={
                    "status": "ERROR",
                    "message": "Hubo un error al enviar su orden"
                }
            )

    # 2. Empaquetar la orden (Convertir el modelo Pydantic a un diccionario/JSON)
    # Le añadimos un estado inicial explícito para que el bus mantenga la trazabilidad
    order_data = order.model_dump()  # Si usas Pydantic v2. Si usas v1, usa order.dict()
    order_data["status"] = "PROCESADA"

    try:
        producer.produce(
            topic='ordenes-procesadas',
            key=str(order_data.get("id", "sin-id")),
            value=json.dumps(order_data).encode('utf-8'),
            callback=delivery_report
        )
        # Le damos un máximo de 1 segundo para vaciar la memoria hacia Kafka
        # Si Kafka está ocupado, Python continuará en lugar de colgar el socket
        producer.flush(timeout=1.0)

    except Exception as e:
        print(f"Error detectado en el bus: {e}")

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "ERROR",
                "message": f"No se pudo registrar en el bus de mensajes: {str(e)}"
            }
        )

    # Retorno exitoso tal como en tu diagrama de secuencia (Paso 5: Confirmación)
    return {
        "status": "RECEIVED",
        "message": "Su orden ha sido enviada"
    }