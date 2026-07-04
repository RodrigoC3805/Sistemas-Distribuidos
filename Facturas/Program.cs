using System;
using System.Text.Json;
using System.Threading;
using Confluent.Kafka;
using MySql.Data.MySqlClient; // 👈 Cambiamos MongoDB por MySQL

namespace ModuloFacturacion
{
    class Program
    {
        private const string BootstrapServers = "20.186.91.190:9092";
        private const string TopicoInput = "ordenes-facturacion";
        
        // Convertimos tus credenciales de TiDB al formato de Connection String estándar
        private const string MySqlConnectionString = "Server=gateway01.us-east-1.prod.aws.tidbcloud.com;Port=4000;Database=test;Uid=2byzAvxahr7Jvuk.root;Pwd=an0ici45DxpIMJX4;SslMode=Required;";

        static void Main(string[] args)
        {
            Console.WriteLine("🚀 Iniciando Módulo de Facturación (.NET Core con TiDB/MySQL)...");

            // Configurar el Consumidor de Kafka
            var conf = new ConsumerConfig
            {
                GroupId = "grupo-modulo-facturacion",
                BootstrapServers = BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
            consumer.Subscribe(TopicoInput);

            Console.WriteLine($"🎧 Escuchando eventos en el tópico [{TopicoInput}]...");
            Console.WriteLine("-------------------------------------------------------");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; 
                cts.Cancel();
            };

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        string mensajeIn = consumeResult.Message.Value;
                        
                        Console.WriteLine($"📥 Mensaje recibido desde Reservas: {mensajeIn}");

                        using JsonDocument doc = JsonDocument.Parse(mensajeIn);
                        JsonElement root = doc.RootElement;

                        string ordenId = root.TryGetProperty("order_id", out var idProp) ? idProp.GetString() ?? "0" : "0";
                        string estadoReserva = root.TryGetProperty("estado_reserva", out var estProp) ? estProp.GetString() ?? "DESCONOCIDO" : "DESCONOCIDO";
                        string detallesJson = root.TryGetProperty("detalles", out var detallesProp) ? detallesProp.ToString() : "[]";

                        Console.WriteLine($"⚙️ Generando factura en TiDB para la Orden ID: {ordenId}...");

                        double subtotal = 25.00; 
                        double impuestos = subtotal * 0.18; 
                        double total = subtotal + impuestos;
                        string numeroFactura = $"FAC-2026-{new Random().Next(1000, 9999)}";

                        // Guardado Relacional Transaccional en TiDB Cloud
                        using (var connection = new MySqlConnection(MySqlConnectionString))
                        {
                            connection.Open();
                            string query = @"INSERT INTO facturas (order_id, numero_factura, fecha_emision, estado_reserva, subtotal, impuestos, total, detalles) 
                                            VALUES (@order_id, @num_fac, @fecha, @estado, @sub, @imp, @tot, @detalles);";

                            using (var cmd = new MySqlCommand(query, connection))
                            {
                                cmd.Parameters.AddWithValue("@order_id", ordenId);
                                cmd.Parameters.AddWithValue("@num_fac", numeroFactura);
                                cmd.Parameters.AddWithValue("@fecha", DateTime.UtcNow);
                                cmd.Parameters.AddWithValue("@estado", estadoReserva);
                                cmd.Parameters.AddWithValue("@sub", subtotal);
                                cmd.Parameters.AddWithValue("@imp", impuestos);
                                cmd.Parameters.AddWithValue("@tot", total);
                                cmd.Parameters.AddWithValue("@detalles", detallesJson);

                                cmd.ExecuteNonQuery();
                            }
                        }

                        Console.WriteLine($"✅ Factura guardada con éxito en TiDB Cloud para la Orden {ordenId}!");
                        Console.WriteLine("🏁 Fin del procesamiento.\n-------------------------------------------------------");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"❌ Error de consumo en Kafka: {e.Error.Reason}");
                    }
                    catch (MySqlException ex)
                    {
                        Console.WriteLine($"❌ Error al insertar en TiDB (MySQL): {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("🛑 Consumidor detenido de forma segura.");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}