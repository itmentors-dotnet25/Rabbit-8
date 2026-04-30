using Domain.Configuration;
using Domain.Events;
using Domain.Interfaces;
using RabbitMQ.Client;
using System.Text;

namespace Web.Endpoints;

public static class TestRabbitEndpoints
{
    public static IEndpointRouteBuilder MapTestRabbitEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/test/rabbit")
                       .WithTags("Test RabbitMQ")
                       .WithOpenApi();

        // 🔹 Публикация тестового сообщения
        group.MapPost("/publish", async (
                InputMessage message,
                IMessageProducer producer,
                RabbitMqSettings settings,
                CancellationToken ct) =>
            {
                await producer.PublishAsync(
                    message, 
                    routingKey: "input", 
                    exchange: settings.ExchangeName, 
                    cancellationToken: ct);

                return Results.Ok(new { published = true, messageId = message.Id });
            })
            .WithName("TestPublishMessage")
            .WithSummary("Публикует сообщение в input-queue для тестов");

        // 🔹 Чтение DLQ для отладки
        group.MapGet("/dlq", (
                IConnection connection,
                RabbitMqSettings settings,
                int maxMessages = 10) =>
            {
                using var channel = connection.CreateModel();
                
                var queueInfo = channel.QueueDeclarePassive(settings.DlqQueueName);
                var available = (int)queueInfo.MessageCount;
                var toFetch = Math.Min(maxMessages, Math.Min(available, 50));
                
                var messages = new List<object>();

                for (int i = 0; i < toFetch; i++)
                {
                    var msg = channel.BasicGet(settings.DlqQueueName, autoAck: false);
                    if (msg == null) break;

                    var payload = Encoding.UTF8.GetString(msg.Body.ToArray());
                    var headers = msg.BasicProperties?.Headers?
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value is byte[] b ? Encoding.UTF8.GetString(b) : kvp.Value?.ToString() ?? "")
                        ?? new Dictionary<string, string>();

                    messages.Add(new { deliveryTag = msg.DeliveryTag, payload, headers, redelivered = msg.Redelivered });
                    
                    // Для тестового вьюера: подтверждаем прочтение (очищаем DLQ)
                    channel.BasicAck(msg.DeliveryTag, multiple: false);
                }

                return Results.Ok(new { queue = settings.DlqQueueName, available, fetched = messages.Count, messages });
            })
            .WithName("TestReadDlq")
            .WithSummary("Читает сообщения из DLQ для отладки");

        return app;
    }
}
