using Domain.Configuration;
using Domain.Events;
using Domain.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Infrastructure.Messaging;

public class OutputQueueConsumer(
    IConnection connection,
    RabbitMqSettings settings,
    ISerializer serializer,
    ILogger<OutputQueueConsumer> logger)
    : BackgroundService
{
    private readonly IConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    private readonly RabbitMqSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly ISerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<OutputQueueConsumer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = _connection.CreateModel();

        // 1. Декларируем топологию (идемпотентно)
        channel.QueueDeclare(_settings.OutputQueueName, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(_settings.OutputQueueName, _settings.ExchangeName, routingKey: "output");

        // 2. Настраиваем потребителя
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            try
            {
                // Десериализация результата
                var result = _serializer.Deserialize<OutputMessage>(body);
                
                // ✅ Финальная обработка результата
                _logger.LogInformation("📤 Output processed: Id={Id} | Status={Status} | Text={Text}", 
                    result.InputMessageId, result.Status, result.ProcessedText);

                // Подтверждение
                channel.BasicAck(ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Failed to process output message. DeliveryTag={Tag}", ea.DeliveryTag);
                channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
            }
            finally
            {
                // ✅ Чистое завершение: сначала закрываем канал, потом освобождаем ресурсы
                // Это гарантирует, что пока мы в try/finally, канал жив для обработчиков
                try
                {
                    channel?.Close();
                    channel?.Dispose();
                    _logger.LogDebug("🔌 Channel disposed");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ Error during channel cleanup");
                }
            }
        };

        // 3. Запуск потребления
        channel.BasicConsume(_settings.OutputQueueName, autoAck: false, consumer: consumer);
        _logger.LogInformation("📤 Output Consumer started. Listening on {Queue}...", _settings.OutputQueueName);

        // 4. Ожидание остановки
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("🛑 Output Consumer stopped gracefully.");
        }
    }
}
