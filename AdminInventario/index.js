const { Kafka } = require('kafkajs');
const { Client } = require('pg');
require('dotenv').config();

// Base de Datos Neon de PostgreSQL
const DATABASE_URL = 'postgresql://neondb_owner:npg_Eab4jKD5oJhg@ep-misty-hall-atly3o4p-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require';

// Conexión al Bus de Kafka en la VM de Azure
const kafka = new Kafka({
  clientId: 'modulo-inventario',
  brokers: ['20.186.91.190:9092'] 
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

      // 1. Capturar ID y datos fiscales enviados desde Python
      const orderId = ordenRecibida.order_id || ordenRecibida.id || 'sin-id';
      const codigoCliente = ordenRecibida.codigo_cliente || 'CLI-001';
      const nombreCliente = ordenRecibida.nombre_cliente || 'Rodrigo Alvarez S.A.C.';
      const rucCliente = ordenRecibida.ruc_cliente || '20765432101';
      
      const items = ordenRecibida.items || ordenRecibida.detalles || [];
      
      let ordenAprobada = true;
      let motivoRechazo = '';
      
      // Lista dinámica para armar el array de ítems enriquecido con datos económicos de la BD
      const detallesEnriquecidos = [];

      // Conexión dinámica a la BD Neon para cada evento
      const pgClient = new Client({ connectionString: DATABASE_URL });
      await pgClient.connect();

      try {
        // Verificar stock y obtener precio unitario de cada item en la orden
        for (const item of items) {
          const codigo = item.code || item.product_id; 
          const cantidadSolicitada = item.quantity;

          console.log(`🔍 Verificando en NeonDB: ${codigo} (Solicitado: ${cantidadSolicitada})`);

          // Traemos la columna 'precio_unitario' desde NeonDB
          const res = await pgClient.query(
            'SELECT nombre_articulo, cantidad_existente, precio_unitario FROM inventario WHERE codigo_articulo = $1',
            [codigo]
          );

          if (res.rows.length === 0) {
            ordenAprobada = false;
            motivoRechazo = `El artículo ${codigo} no existe en el catálogo.`;
            break;
          }

          const productoBD = res.rows[0];
          
          // Validación lógica de Stock
          if (productoBD.cantidad_existente < cantidadSolicitada) {
            ordenAprobada = false;
            motivoRechazo = `Stock insuficiente para ${productoBD.nombre_articulo}. Requerido: ${cantidadSolicitada}, Disponible: ${productoBD.cantidad_existente}`;
            break;
          }
          
          // Cálculos económicos reales obtenidos de la BD
          const precioUnitario = parseFloat(productoBD.precio_unitario || 0.00);
          const subtotalItem = cantidadSolicitada * precioUnitario;

          // Construimos el objeto del ítem estructurado para la rúbrica de .NET
          detallesEnriquecidos.push({
            code: codigo,
            name: productoBD.nombre_articulo,
            quantity: cantidadSolicitada,
            price: precioUnitario,     // 💸 Inyectado desde NeonDB
            subtotal: subtotalItem     // 💸 Calculado en caliente
          });
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
        // 🟩 CAMINO FELIZ: Corregido a tu tópico original 'inventario-verificado' 
        const eventoInventario = {
          order_id: orderId,
          codigo_cliente: codigoCliente,
          nombre_cliente: nombreCliente,
          ruc_cliente: rucCliente,
          status: 'STOCK_VERIFIED',
          timestamp: Date.now(),
          detalles: detallesEnriquecidos // Incluye precios y subtotales fidedignos
        };

        await producer.send({
          topic: 'inventario-verificado', // 👈 ¡Devuelto a su lugar correcto!
          messages: [
            { key: orderId.toString(), value: JSON.stringify(eventoInventario) }
          ]
        });
        console.log(`🟩 ORDEN APROBADA: Mensaje enviado al tópico 'inventario-verificado'`);

      } else {
        // 🟥 CAMINO DE RECHAZO: Envía la cancelación al tópico general de control
        const eventoRechazo = {
          order_id: orderId,
          status: 'REJECTED',
          motivo: motivoRechazo,
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