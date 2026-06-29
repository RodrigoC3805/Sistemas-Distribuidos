const { Kafka } = require('kafkajs');
const { Client } = require('pg');
require('dotenv').config();

const DATABASE_URL = 'postgresql://neondb_owner:npg_Eab4jKD5oJhg@ep-misty-hall-atly3o4p-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require';

// 2. Conexión al Bus de Kafka Local (Tu IP Wi-Fi)
const kafka = new Kafka({
  clientId: 'modulo-inventario',
  brokers: ['192.168.1.49:9092'] 
});

const consumer = kafka.consumer({ groupId: 'grupo-inventario' });
const producer = kafka.producer();

const iniciarModulo = async () => {
  await consumer.connect();
  await producer.connect();
  console.log('🚀 Módulo de Inventario (Node.js) escuchando órdenes...');

  // Escuchar el tópico que escribe Python
  await consumer.subscribe({ topic: 'ordenes-procesadas', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const ordenRecibida = JSON.parse(message.value.toString());
      console.log(`\n📦 Nueva orden detectada en el Bus:`, ordenRecibida);

      // Capturamos el ID secuencial que generó Python en NeonDB
      const orderId = ordenRecibida.id || 'sin-id';
      const items = ordenRecibida.items || [];
      let ordenAprobada = true;
      let motivoRechazo = '';

      // Conexión dinámica a la BD Neon para cada evento
      const pgClient = new Client({ connectionString: DATABASE_URL });
      await pgClient.connect();

      try {
        // Verificar stock de cada item en la orden
        for (const item of items) {
          const codigo = item.code || item.product_id; 
          const cantidadSolicitada = item.quantity;

          console.log(`🔍 Verificando en NeonDB: ${codigo} (Solicitado: ${cantidadSolicitada})`);

          // Consulta SQL directa a Neon
          const res = await pgClient.query(
            'SELECT nombre_articulo, cantidad_existente FROM inventario WHERE codigo_articulo = $1',
            [codigo]
          );

          if (res.rows.length === 0) {
            ordenAprobada = false;
            motivoRechazo = `El artículo ${codigo} no existe en el catálogo.`;
            break;
          }

          const productoBD = res.rows[0];
          
          if (productoBD.cantidad_existente < cantidadSolicitada) {
            ordenAprobada = false;
            motivoRechazo = `Stock insuficiente para ${productoBD.nombre_articulo}. Requerido: ${cantidadSolicitada}, Disponible: ${productoBD.cantidad_existente}`;
            break;
          }
          
          // OPCIONAL: Descomentar para restar stock real si pasa la validación
          // await pgClient.query('UPDATE inventario SET cantidad_existente = cantidad_existente - $1 WHERE codigo_articulo = $2', [cantidadSolicitada, codigo]);
        }
      } catch (err) {
        console.error('❌ Error consultando la base de datos Neon:', err);
        ordenAprobada = false;
        motivoRechazo = 'Error interno en el servidor de inventario.';
      } finally {
        await pgClient.end(); // Cerrar conexión de BD de forma segura
      }

      // 3. Notificar el resultado de vuelta al Bus de Kafka según el veredicto
      if (ordenAprobada) {
        // 🟩 CAMINO FELIZ: Continúa al Módulo 3 a través de 'inventario-verificado'
        const eventoInventario = {
          order_id: orderId,
          status: 'STOCK_VERIFIED',
          timestamp: Date.now(),
          detalles: items
        };

        await producer.send({
          topic: 'inventario-verificado',
          messages: [
            { key: orderId.toString(), value: JSON.stringify(eventoInventario) }
          ]
        });
        console.log(`🟩 ORDEN APROBADA: Mensaje enviado al tópico 'inventario-verificado'`);

      } else {
        // 🟥 CAMINO DE RECHAZO: Bypass directo al tópico de control 'ordenes-status'
        const eventoRechazo = {
          order_id: orderId,
          status: 'REJECTED',
          timestamp: Date.now()
        };

        await producer.send({
          topic: 'ordenes-status',
          messages: [
            { key: orderId.toString(), value: JSON.stringify(eventoRechazo) }
          ]
        });
        console.log(`🟥 ORDEN RECHAZADA: ${motivoRechazo}. Notificado a 'ordenes-status'`);
      }
    },
  });
};

iniciarModulo().catch(console.error);