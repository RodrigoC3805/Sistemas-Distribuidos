package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go" // Driver en Go Puro
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	BootstrapServers = "20.186.91.190:9092"
	TopicoInput      = "facturas-emitidas"
	TopicoOutput     = "ordenes-status" // Tópico para notificar a Procesamiento de Órdenes (Python)
	MongoURI         = "mongodb+srv://db_user:O17iK7i0tIPxlhAY@cluster0.isd69p8.mongodb.net/?appName=Cluster0&tls=true&tlsAllowInvalidCertificates=true"
)

// ItemDetalle representa la estructura para capturar de 1 a n productos
type ItemDetalle struct {
	Descripcion    string  `json:"descripcion" bson:"descripcion"`
	Cantidad       int     `json:"cantidad" bson:"cantidad"`
	PrecioUnitario float64 `json:"precio_unitario" bson:"precio_unitario"`
	Subtotal       float64 `json:"subtotal" bson:"subtotal"`
}

// CuentaPorCobrar estructurado con los nombres exactos del JSON recibido de .NET
type CuentaPorCobrar struct {
	OrderID        string        `json:"order_id" bson:"order_id"`
	NumeroFactura  string        `json:"numero_factura" bson:"numero_factura"`
	CodigoCliente  string        `json:"codigo_cliente" bson:"codigo_cliente"`
	NombreCliente  string        `json:"nombre_cliente" bson:"nombre_cliente"`
	RucCliente     string        `json:"ruc_cliente" bson:"ruc_cliente"`
	Detalles       []ItemDetalle `json:"detalles" bson:"detalles"` // Array dinámico de 1 a n ítems
	TotalIgv       float64       `json:"total_igv" bson:"total_igv"`
	TotalFactura   float64       `json:"total_factura" bson:"total_x_cobrar"` // Total x cobrar
	FechaCobro     time.Time     `json:"-" bson:"fecha_cobro"`                // Seteado internamente en Go
	EstadoRegistro string        `json:"estado_pago" bson:"estado_registro"`  // 'PENDIENTE'
}

// EventoNotificacion incluye ahora el número de factura y la fecha estimada de entrega
type EventoNotificacion struct {
	OrderID       string `json:"order_id"`
	Status        string `json:"status"`
	NumeroFactura string `json:"num_factura"`    // 📝 Agregado
	FechaEntrega  string `json:"fecha_entrega"`  // 📝 Agregado
	Mensaje       string `json:"mensaje"`
	Timestamp     int64  `json:"timestamp"`
}

func main() {
	fmt.Println("🚀 Iniciando Módulo de Cuentas por Cobrar Avanzado (Go Puro)...")

	// 1. Conexión limpia a MongoDB Atlas con bypass de TLS
	ctxMongo, cancelMongo := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMongo()

	clientOptions := options.Client().ApplyURI(MongoURI).SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	mongoClient, err := mongo.Connect(ctxMongo, clientOptions)
	if err != nil {
		log.Fatalf("❌ Error al conectar a MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctxMongo)

	collection := mongoClient.Database("cuentasxcobrar-db").Collection("cuentasxcobrar")
	fmt.Println("💾 Conexión exitosa con MongoDB Atlas (0.0.0.0/0 Activo).")

	// 2. Configurar el Consumidor de Kafka (Lee de .NET)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{BootstrapServers},
		GroupID:  "grupo-cuentas-cobrar",
		Topic:    TopicoInput,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	// 3. Configurar el Productor de Kafka (Escribe a Procesamiento de Órdenes)
	w := &kafka.Writer{
		Addr:     kafka.TCP(BootstrapServers),
		Topic:    TopicoOutput,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	fmt.Printf("🎧 Escuchando en [%s] y notificando éxito en [%s]...\n", TopicoInput, TopicoOutput)
	fmt.Println("-------------------------------------------------------")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigchan
		fmt.Println("\n🛑 Deteniendo módulo de Go de forma segura...")
		cancel()
	}()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Printf("❌ Error al leer mensaje de Kafka: %v\n", err)
			continue
		}

		fmt.Printf("📥 Mensaje JSON recibido desde .NET:\n%s\n", string(msg.Value))

		var cuenta CuentaPorCobrar
		err = json.Unmarshal(msg.Value, &cuenta)
		if err != nil {
			fmt.Printf("⚠️ Error al deserializar JSON: %v\n", err)
			continue
		}

		cuenta.FechaCobro = time.Now()
		if cuenta.EstadoRegistro == "" {
			cuenta.EstadoRegistro = "PENDIENTE"
		}

		fmt.Printf("⚙️ Registrando cuenta por cobrar para la Factura: %s...\n", cuenta.NumeroFactura)

		// 4. PERSISTENCIA EN MONGODB ATLAS
		_, err = collection.InsertOne(context.Background(), cuenta)
		if err != nil {
			fmt.Printf("❌ Error al guardar en MongoDB Atlas: %v\n", err)
			continue
		}
		fmt.Printf("✅ ¡Factura completa guardada en Atlas!\n")

		// 5. CÁLCULO DE FECHA DE ENTREGA SIMULADA (Hoy + 3 días)
		fechaEstimada := time.Now().AddDate(0, 0, 3)
		fechaEntregaStr := fechaEstimada.Format("2006-01-02") // Formato estándar YYYY-MM-DD

		// 6. ENVIAR MENSAJE DE APROBACIÓN CON CAMPOS REQUERIDOS (iii)
		textoMensaje := fmt.Sprintf("Estimado cliente de %s, su pedido con Orden ID %s ha sido ejecutado satisfactoriamente. Se le enviará el mismo en la fecha %s conjuntamente con la respectiva factura %s.", 
			cuenta.NombreCliente, cuenta.OrderID, fechaEntregaStr, cuenta.NumeroFactura)

		notificacion := EventoNotificacion{
			OrderID:       cuenta.OrderID,
			Status:        "COMPLETED",
			NumeroFactura: cuenta.NumeroFactura, // Inyectado
			FechaEntrega:  fechaEntregaStr,      // Inyectado
			Mensaje:       textoMensaje,
			Timestamp:     time.Now().UnixMilli(),
		}

		jsonNotificacion, _ := json.Marshal(notificacion)

		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(cuenta.OrderID),
				Value: jsonNotificacion,
			},
		)

		if err != nil {
			fmt.Printf("❌ Error enviando notificación de éxito a Kafka: %v\n", err)
		} else {
			fmt.Printf("🚀 Notificación enviada a ordenes-status con Factura %s y Entrega %s.\n", cuenta.NumeroFactura, fechaEntregaStr)
			fmt.Println("🏁 Fin del procesamiento del ciclo.\n-------------------------------------------------------")
		}
	}
	fmt.Println("🏁 Módulo de Go cerrado correctamente.")
}