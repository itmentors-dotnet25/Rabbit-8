using Application.Interfaces;
using Domain.Configuration;
using Domain.Interfaces;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Infrastructure.Messaging;

public class RabbitMqProducer(
    IConnection connection,
    RabbitMqSettings settings,
    ILogger<InputQueueConsumer> logger,
    ISerializer serializer,
    IMetricsCollector metrics)
    : IMessageProducer, IDisposable
{
    private readonly IConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    private readonly RabbitMqSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly ISerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private IModel? _channel;
    private readonly ILogger<InputQueueConsumer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly IMetricsCollector _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));


    private IModel GetChannel() => 
        _channel ??= _connection.CreateModel();

    public Task PublishAsync<T>(
        T message, 
        string routingKey, 
        string? exchange = null,
        CancellationToken cancellationToken = default)
    {
        var channel = GetChannel();
        
        var targetExchange = exchange ?? _settings.ExchangeName;
    
        // 🔍 Логирование для отладки
        _logger?.LogInformation("📤 Publishing: exchange='{Exchange}', routingKey='{Key}', type={Type}", 
            targetExchange, routingKey, typeof(T).Name);
        
        // Декларируем обменник (идемпотентно)
        channel.ExchangeDeclare(
            exchange: targetExchange,
            type: _settings.ExchangeType,
            durable: true);

        // Сериализуем сообщение
        var body = _serializer.Serialize(message);

        // Настраиваем свойства доставки
        var properties = channel.CreateBasicProperties();
        properties.Persistent = true; // Сохранение на диск
        properties.ContentType = "application/json";
        properties.Timestamp = new AmqpTimestamp((long)(DateTime.UtcNow - DateTime.UnixEpoch).TotalSeconds);

        // Публикуем
        channel.BasicPublish(
            exchange: targetExchange,
            routingKey: routingKey,
            mandatory: true, // Вернуть, если некуда маршрутизировать
            basicProperties: properties,
            body: body);
        
        // Добавляем в метрику
        _metrics.RecordMessagePublished(
            queueName: routingKey, 
            messageType: typeof(T).Name);

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _channel?.Close();
        _channel?.Dispose();
    }
}
