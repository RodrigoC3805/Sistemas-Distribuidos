package com.example.modulo_reservas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Service
public class ReservasWorker {

    private static final Logger log = LoggerFactory.getLogger(ReservasWorker.class);
    private static final String TOPICO_SALIDA = "ordenes-facturacion";
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate; // Conector a tu base de datos Neon

    // Escucha exactamente "inventario-verificado" que envía Node.js
    @KafkaListener(topics = "inventario-verificado", groupId = "grupo-reservas")
    public void procesarReserva(String mensajeIn) {
        try {
            log.info("📦 Mensaje recibido desde Inventario: {}", mensajeIn);
            JsonNode datosOrden = objectMapper.readTree(mensajeIn);
            
            // Usamos path() en lugar de get() para evitar NullPointerException si el JSON cambia
            String ordenId = datosOrden.path("order_id").asText("sin-id");
            JsonNode items = datosOrden.path("detalles");

            log.info("⚙️ Procesando Reserva para la Orden ID: {}", ordenId);

            // 1. Modificar la base de datos de reservas (Tu lógica física existente)
            if (!items.isMissingNode() && items.isArray()) {
                actualizarBaseDeDatos(items);
            } else {
                log.warn("⚠️ La orden no contiene la lista de 'detalles' de productos.");
            }

            // 2. El Pasamanos Perfecto: Convertimos a ObjectNode mutable
            // Esto mantiene vivos: codigo_cliente, nombre_cliente, ruc_cliente, price y subtotal
            ObjectNode mensajeFacturacion = (ObjectNode) datosOrden;
            
            // Agregamos la firma de este módulo para la rúbrica
            mensajeFacturacion.put("estado_reserva", "COMPLETADA");
            mensajeFacturacion.put("timestamp_reserva", System.currentTimeMillis());

            // 3. Serializar todo el JSON enriquecido de vuelta a String
            String mensajeOut = objectMapper.writeValueAsString(mensajeFacturacion);
            
            // 4. Enviar a .NET usando el orderId como KEY de partición
            kafkaTemplate.send(TOPICO_SALIDA, ordenId, mensajeOut).whenComplete((resultado, ex) -> {
                if (ex != null) {
                    log.error("❌ Error al enviar a Facturación: {}", ex.getMessage());
                } else {
                    log.info("🚀 Evento completo (Clientes + Precios de BD) retransmitido a Facturación en [{}]", TOPICO_SALIDA);
                }
            });

            log.info("🏁 Fin de procesamiento para Orden {}.\n-----------------------------------", ordenId);

        } catch (Exception e) {
            log.error("❌ Error inesperado en el flujo de reservas: {}", e.getMessage());
        }
    }

    private void actualizarBaseDeDatos(JsonNode items) {
        log.info("💾 Conectando a la Base de Datos para actualizar existencias...");
        
        for (JsonNode item : items) {
            // Soportamos que el código venga como 'code' o 'product_id'
            String articuloId = item.path("code").isMissingNode() ? item.path("product_id").asText() : item.path("code").asText();
            int cantidadSolicitada = item.path("quantity").asInt(0);
            
            // Tu query SQL real
            String sql = "UPDATE inventario SET cantidad_existente = cantidad_existente - ? WHERE codigo_articulo = ?";
            
            try {
                int filasAfectadas = jdbcTemplate.update(sql, cantidadSolicitada, articuloId);
                
                if (filasAfectadas > 0) {
                    log.info("📉 Stock disminuido en BD: Artículo {} en -{} unidades.", articuloId, cantidadSolicitada);
                } else {
                    log.warn("⚠️ No se encontró el artículo {} en la base de datos.", articuloId);
                }
            } catch (Exception e) {
                log.error("❌ Error al ejecutar el UPDATE en la BD: {}", e.getMessage());
            }
        }
        log.info("✅ Proceso de actualización de Base de Datos finalizado.");
    }
}