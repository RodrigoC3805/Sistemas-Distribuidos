import json
import threading
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool  # Soportar hilos de FastAPI + Worker de Kafka
from fastapi import FastAPI, HTTPException
from app.schemas import OrderRequest
from confluent_kafka import Producer, Consumer

app = FastAPI(title="Módulo 1: Procesamiento de Órdenes")

# 🔏 URL OFICIAL CON REGLAS DE RED OPTIMIZADAS PARA PYTHON
DATABASE_URL = 'postgresql://neondb_owner:npg_Eab4jKD5oJhg@ep-misty-hall-atly3o4p-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=prefer&gssencmode=disable'

# Inicialización del Pool Seguro para Multihilo (Ajustado para no saturar al pooler de Neon)
try:
    db_pool = ThreadedConnectionPool(1, 4, DATABASE_URL, connect_timeout=5)
    print("✅ ThreadedConnectionPool (Optimizado para Neon Pooler) inicializado con éxito.")
except Exception as e:
    print(f"💥 Error fatal al inicializar el Pool de BD: {e}")
    db_pool = None

# Configuración de Kafka Local (Tu IP Wi-Fi)
KAFKA_BROKER = '192.168.1.49:9092'
kafka_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'modulo-procesamiento-ordenes',
    'socket.timeout.ms': 3000
}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Kafka delivery error: {err}")
    else:
        print(f"🟩 Mensaje entregado con éxito a {msg.topic()} [Partición: {msg.partition()}]")

# --- ENDPOINTS ---

@app.post("/orders")
def create_order(order: OrderRequest):
    print("\n📥 [POST /orders] Petición recibida. Iniciando validaciones...")
    
    # Validaciones de negocio
    if not order.items:
        raise HTTPException(status_code=400, detail={"status": "ERROR", "message": "Hubo un error al enviar su orden"})
    for item in order.items:
        if item.quantity <= 0:
            raise HTTPException(status_code=422, detail={"status": "ERROR", "message": "Hubo un error al enviar su orden"})

    print("✅ Validaciones de esquema superadas.")

    items_list = [item.dict() for item in order.items]
    items_json = json.dumps(items_list)
    fecha_actual = datetime.now()

    order_id_str = None
    conn = None
    print("⏳ POST: Solicitando conexión rápida al Pool...")
    try:
        conn = db_pool.getconn()  # Toma una conexión caliente del pool de inmediato
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO registro_ordenes (items, estado, fecha_registro) 
            VALUES (%s, %s, %s) RETURNING id
            """,
            (items_json, "PROCESANDO", fecha_actual)
        )
        
        generated_id = cursor.fetchone()[0]
        order_id_str = str(generated_id)

        conn.commit()
        cursor.close()
        print(f"💾 BD ÉXITO: Orden #{order_id_str} guardada en NeonDB.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"💥 ERROR EN POST NEONDB: {str(e)}")
        raise HTTPException(status_code=500, detail={"status": "ERROR", "message": f"Error de persistencia: {str(e)}"})
    finally:
        if conn:
            db_pool.putconn(conn)  # Devuelve la conexión al pool inmediatamente para liberar el socket

    # 2. Publicar el Evento en Kafka
    print(f"🛰️ Publicando Orden #{order_id_str} en Kafka...")
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
        print("📨 Evento enviado al buffer de Kafka.")
    except Exception as e:
        print(f"💥 ERROR KAFKA: {e}")
        raise HTTPException(status_code=500, detail={"status": "ERROR", "message": f"Error en bus: {str(e)}"})

    return {
        "status": "RECEIVED", 
        "order_id": order_id_str, 
        "message": "Su orden ha sido enviada"
    }


@app.get("/orders")
def get_orders():
    conn = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT id, items, estado, fecha_registro, num_factura, fecha_entrega FROM registro_ordenes ORDER BY id ASC")
        rows = cursor.fetchall()
        cursor.close()
        
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
        raise HTTPException(status_code=500, detail={"status": "ERROR", "message": str(e)})
    finally:
        if conn:
            db_pool.putconn(conn)


# --- WORKER CONSUMIDOR ASÍNCRONO ---
def kafka_consumer_worker():
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'grupo-procesamiento-ordenes-status',
        'auto.offset.reset': 'latest',
        'socket.timeout.ms': 3000
    }
    
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe(['ordenes-status']) 
        print("🎧 Worker de Python activo y escuchando 'ordenes-status'...")
    except Exception as e:
        print(f"💥 Error al iniciar el consumidor de Kafka: {e}")
        return
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Alerta Worker Kafka: {msg.error()}")
            continue

        try:
            evento = json.loads(msg.value().decode('utf-8'))
            order_id = int(evento.get("order_id"))
            status = evento.get("status")
            
            print(f"📥 Worker capturó actualización para Orden #{order_id} [{status}]")

            # El worker solicita de forma segura su propia conexión aislada al pool
            conn = db_pool.getconn()
            cursor = conn.cursor()

            if status == "REJECTED":
                cursor.execute("UPDATE registro_ordenes SET estado = 'RECHAZADA' WHERE id = %s", (order_id,))
                print(f"🔄 BD Actualizada por Worker: Orden #{order_id} -> RECHAZADA.")
            elif status == "COMPLETED":
                num_factura = evento.get("num_factura")
                fecha_entrega = evento.get("fecha_entrega")
                cursor.execute(
                    "UPDATE registro_ordenes SET estado = 'COMPLETADA', num_factura = %s, fecha_entrega = %s WHERE id = %s",
                    (num_factura, fecha_entrega, order_id)
                )
                print(f"🔄 BD Actualizada por Worker: Orden #{order_id} -> COMPLETADA.")

            conn.commit()
            cursor.close()
            db_pool.putconn(conn)  # Liberar la conexión asíncrona de inmediato

        except Exception as e:
            print(f"⚠️ Error interno en Worker asíncrono: {e}")

# Arrancar el hilo en background de forma limpia
threading.Thread(target=kafka_consumer_worker, daemon=True).start()