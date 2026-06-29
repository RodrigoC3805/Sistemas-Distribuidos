import json
import threading
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from app.schemas import OrderRequest
from confluent_kafka import Producer, Consumer

app = FastAPI(title="Módulo 1: Procesamiento de Órdenes")

DATABASE_URL = 'postgresql://neondb_owner:npg_lwWYMGDA58kE@ep-withered-lab-aimrndoh-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

# Configuración de Kafka (Apunta a tu IP Wi-Fi local)
KAFKA_BROKER = '192.168.1.49:9092'

kafka_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'modulo-procesamiento-ordenes'
}

producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al entregar mensaje: {err}")
    else:
        print(f"Mensaje entregado con éxito a {msg.topic()} [Partición: {msg.partition()}]")

# --- ENDPOINTS ---

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

    # 1. Preparar datos para NeonDB
    items_list = [item.dict() for item in order.items]
    items_json = json.dumps(items_list)
    fecha_actual = datetime.now()

    # 2. Persistencia en la tabla 'registro_ordenes' usando el ID Serial de Neon
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO registro_ordenes (items, estado, fecha_registro) 
            VALUES (%s, %s, %s) RETURNING id
            """,
            (items_json, "PROCESANDO", fecha_actual)
        )
        
        generated_id = cursor.fetchone()[0]
        order_id_str = str(generated_id)  # ID secuencial definitivo (1, 2, 3...)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Orden #{order_id_str} registrada en NeonDB.")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "ERROR",
                "message": f"Error de persistencia en la base de datos: {str(e)}"
            }
        )

    # 3. Empaquetar la orden con el ID correcto y enviarla a Kafka
    order_data = {
        "id": order_id_str,
        "items": items_list,
        "status": "PROCESADA"
    }

    try:
        producer.produce(
            topic='ordenes-procesadas',
            key=order_id_str,
            value=json.dumps(order_data).encode('utf-8'),
            callback=delivery_report
        )
        producer.flush(timeout=1.0)

    except Exception as e:
        # Si falla Kafka, se podría manejar un rollback, pero aquí se reporta el error de bus
        print(f"Error detectado en el bus: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "ERROR",
                "message": f"No se pudo registrar en el bus de mensajes: {str(e)}"
            }
        )

    # Retorno exitoso enviando de vuelta el ID secuencial real generado
    return {
        "status": "RECEIVED",
        "order_id": order_id_str,
        "message": "Su orden ha sido enviada"
    }


@app.get("/orders")
def get_orders():
    """Endpoint para enviar la lista de órdenes completa e histórica a la app móvil"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, items, estado, fecha_registro, num_factura, fecha_entrega FROM registro_ordenes ORDER BY id ASC")
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()

        resultado = []
        for row in rows:
            resultado.append({
                "order_id": str(row['id']),
                "items": row['items'] if isinstance(row['items'], list) else json.loads(row['items']),
                "estado": row['estado'],
                "fecha_registro": row['fecha_registro'].isoformat() if row['fecha_registro'] else None,
                "num_factura": row['num_factura'],
                "fecha_entrega": row['fecha_entrega']
            })
        return resultado

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "ERROR",
                "message": f"No se pudo obtener la lista de órdenes: {str(e)}"
            }
        )

# --- WORKER CONSUMIDOR EN SEGUNDO PLANO (HILO APARTE) ---
def kafka_consumer_worker():
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'grupo-procesamiento-ordenes-status',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_config)
    # Escucha el nuevo tópico correcto de control unificado
    consumer.subscribe(['ordenes-status']) 

    print("🎧 Worker de Python activo y escuchando cambios en 'ordenes-status'...")
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        try:
            evento = json.loads(msg.value().decode('utf-8'))
            order_id = int(evento.get("order_id"))
            status = evento.get("status")

            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()

            if status == "REJECTED":
                # Si llega de Node.js, cambia a 'RECHAZADA' sin guardar motivo
                cursor.execute(
                    "UPDATE registro_ordenes SET estado = 'RECHAZADA' WHERE id = %s",
                    (order_id,)
                )
                print(f"❌ Orden #{order_id} actualizada a RECHAZADA en NeonDB.")
            
            elif status == "COMPLETED":
                # Si llega del módulo final, actualiza con los campos de la factura
                num_factura = evento.get("num_factura")
                fecha_entrega = evento.get("fecha_entrega")
                cursor.execute(
                    "UPDATE registro_ordenes SET estado = 'COMPLETADA', num_factura = %s, fecha_entrega = %s WHERE id = %s",
                    (num_factura, fecha_entrega, order_id)
                )
                print(f"🎉 Orden #{order_id} actualizada a COMPLETADA en NeonDB.")

            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"⚠️ Error en el Worker asíncrono de actualización: {e}")

# Iniciar la escucha asíncrona de Kafka en background al encender FastAPI
threading.Thread(target=kafka_consumer_worker, daemon=True).start()