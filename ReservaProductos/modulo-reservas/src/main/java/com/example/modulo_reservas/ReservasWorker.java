package com.example.modulo_reservas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service; // 💻 Ahora sí se debe reconocer

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
    private JdbcTemplate jdbcTemplate; // 💾 Conector a tu base de datos

    // 🔍 CORREGIDO: Ahora escucha exactamente "inventario-verificado"
    @KafkaListener(topics = "inventario-verificado")
    public void procesarReserva(String mensajeIn) {
        try {
            log.info("📦 Mensaje recibido desde Inventario: {}", mensajeIn);
            JsonNode datosOrden = objectMapper.readTree(mensajeIn);
            
            String ordenId = datosOrden.get("order_id").asText();
            JsonNode items = datosOrden.get("detalles");

            log.info("⚙️ Procesando Reserva para la Orden ID: {}", ordenId);

            if (items != null) {
                actualizarBaseDeDatos(items);
            } else {
                log.warn("⚠️ La orden no contiene la lista de 'detalles' de productos.");
            }

            // Preparar envío a Facturación
            ObjectNode mensajeFacturacion = (ObjectNode) datosOrden;
            mensajeFacturacion.put("estado_reserva", "COMPLETADA");

            String mensajeOut = objectMapper.writeValueAsString(mensajeFacturacion);
            
            kafkaTemplate.send(TOPICO_SALIDA, mensajeOut).whenComplete((resultado, ex) -> {
                if (ex != null) {
                    log.error("❌ Error al enviar a Facturación: {}", ex.getMessage());
                } else {
                    log.info("🚀 Evento enviado a Facturación en el tópico [{}]", TOPICO_SALIDA);
                }
            });

            log.info("🏁 Fin de procesamiento para Orden {}.\n-----------------------------------", ordenId);

        } catch (Exception e) {
            log.error("❌ Error inesperado en el flujo de reservas: {}", e.getMessage());
        }
    }

    private void actualizarBaseDeDatos(JsonNode items) {
        log.info("💾 Conectando a la Base de Datos para actualizar existencias...");
        if (items.isArray()) {
            for (JsonNode item : items) {
                String articuloId = item.get("code").asText();
                int cantidadSolicitada = item.get("quantity").asInt();
                
                // 🛠️ Ejecución SQL Real
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
        }
        log.info("✅ Proceso de actualización de Base de Datos finalizado.");
    }
}