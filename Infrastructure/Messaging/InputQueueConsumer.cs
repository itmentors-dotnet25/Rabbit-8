using Application.Interfaces;
using Domain.Configuration;
using Domain.Events;
using Domain.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Infrastructure.Messaging;

public class InputQueueConsumer(
    IConnection connection,
    RabbitMqSettings settings,
    ISerializer serializer,
    ILogger<InputQueueConsumer> logger,
    IMessageProducer producer,
    IIncomingMessageHandler messageHandler,
    IMetricsCollector metrics,
    ResiliencePipeline resiliencePipeline)  // ← Polly policy injected
    : BackgroundService
{
    private readonly IConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    private readonly RabbitMqSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly ISerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<InputQueueConsumer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly IMessageProducer _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    private readonly IIncomingMessageHandler _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));
    private readonly IMetricsCollector _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    private readonly ResiliencePipeline _resiliencePipeline = resiliencePipeline ?? throw new ArgumentNullException(nameof(resiliencePipeline));

    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = _connection.CreateModel();
        
        // 1. Декларируем топологию (включая DLX)
        DeclareQueueTopology(channel);

        // 2. Создаём и настраиваем потребителя
        var consumer = CreateConsumer(channel, stoppingToken);
        
        // 3. Запускаем потребление
        channel.BasicConsume(
            queue: _settings.InputQueueName,
            autoAck: false,
            consumer: consumer);

        _logger.LogInformation("🐇 Input Consumer started. Listening on {Queue}...", _settings.InputQueueName);

        // 4. Ждём остановки
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("🛑 Input Consumer stopped gracefully.");
        }
        finally
        {
            // ✅ Закрываем канал ПОСЛЕ того, как перестали поступать новые сообщения.
            // Даём обработчикам время завершиться
            try
            {
                channel?.Close();
                channel?.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Error closing channel");
            }
        }
    }

    private void DeclareQueueTopology(IModel channel)
    {
        try
        {
            // 🔹 DLX-инфраструктура
            var dlxExchangeName = $"{_settings.ExchangeName}.dlx";
            
            channel.ExchangeDeclare(dlxExchangeName, ExchangeType.Direct, durable: true);
            channel.QueueDeclare(_settings.DlqQueueName, durable: true, exclusive: false, autoDelete: false);
            channel.QueueBind(_settings.DlqQueueName, dlxExchangeName, routingKey: "dlq");
            _logger.LogDebug("✅ DLX topology declared: {DlxExchange} → {DlqQueue}", dlxExchangeName, _settings.DlqQueueName);

            // 🔹 Основная очередь с аргументами DLQ
            var inputQueueArgs = new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", dlxExchangeName },
                { "x-dead-letter-routing-key", "dlq" }
            };
            
            channel.ExchangeDeclare(_settings.ExchangeName, _settings.ExchangeType, durable: true);
            channel.QueueDeclare(_settings.InputQueueName, durable: true, exclusive: false, autoDelete: false, arguments: inputQueueArgs);
            channel.QueueBind(_settings.InputQueueName, _settings.ExchangeName, routingKey: "input");
            _logger.LogDebug("✅ Input queue declared with DLX: {Queue}", _settings.InputQueueName);
            
            // 🔹 Выходная очередь
            channel.QueueDeclare(_settings.OutputQueueName, durable: true, exclusive: false, autoDelete: false);
            channel.QueueBind(_settings.OutputQueueName, _settings.ExchangeName, routingKey: "output");
            _logger.LogDebug("✅ Output queue declared: {Queue}", _settings.OutputQueueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "❌ Failed to declare topology");
            throw;
        }
    }

private EventingBasicConsumer CreateConsumer(IModel channel, CancellationToken stoppingToken)
{
    var consumer = new EventingBasicConsumer(channel);
    
    consumer.Received += (model, ea) =>
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        // 🔥 Лог ДО любых операций — если его нет, событие не срабатывает
        _logger.LogInformation("🔔 [RECEIVED] DeliveryTag={Tag}, RoutingKey={Key}, BodyLen={Len}", 
            ea.DeliveryTag, ea.RoutingKey, ea.Body.Length);
    
        InputMessage? message = null;
    
        try
        {
            var body = ea.Body.ToArray();
            _logger.LogDebug("🔍 Deserializing {Bytes} bytes...", body.Length);
            
            // Десериализация (синхронная)
            message = _serializer.Deserialize<InputMessage>(body);
            _logger.LogInformation("📥 Received message: {Id} | {Text}", message.Id, message.Text);

            // 🔹 🔹 🔹 POLLY v8: выполняем бизнес-логику с повторами 🔹 🔹 🔹
            // Используем ExecuteAsync, т.к. HandleAsync — асинхронный метод.
            // Блокируем поток ожидания ТОЛЬКО снаружи, чтобы не ломать внутренние await.
            var result = _resiliencePipeline
                .ExecuteAsync(
                    async ct => await _messageHandler.HandleAsync(message, ct),
                    stoppingToken)
                .GetAwaiter()
                .GetResult();

            // 📤 Инфраструктура публикует результат, полученный из Application
            _producer.PublishAsync(result, routingKey: "output", cancellationToken: stoppingToken)
                     .GetAwaiter()
                     .GetResult();

            // ✅ Если Polly не выбросил → всё успешно
            channel.BasicAck(ea.DeliveryTag, multiple: false);
            _logger.LogInformation("✅ Ack sent for message {Id}", message.Id);
            
            // ✅ Добавление в метрику
            stopwatch.Stop();
            _metrics.RecordMessageProcessed(
                queueName: _settings.InputQueueName,
                messageType: nameof(InputMessage),
                processingTimeMs: stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            // ❌ Polly исчерпал повторы ИЛИ ошибка до начала повторов (десериализация)
            _logger.LogError(ex, "💥 Failed to process message. DeliveryTag={Tag}, MessageId={MsgId}", 
                ea.DeliveryTag, message?.Id);
            
            // ❌ Nack с requeue: false → RabbitMQ сам перенаправит в DLQ
            // (благодаря аргументам x-dead-letter-*, заданным в DeclareQueueTopology)
            try
            {
                channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                _logger.LogWarning("⚠️ Message nack'ed (requeue: false) → routing to DLQ");
            }
            catch (Exception nackEx)
            {
                _logger.LogError(nackEx, "💥 Failed to send Nack! DeliveryTag={Tag}", ea.DeliveryTag);
            }
            stopwatch.Stop();
            _metrics.RecordMessageError(
                queueName: _settings.InputQueueName,
                messageType: nameof(InputMessage),
                errorType: ex.GetType().Name);
            throw;
        }
    };

    return consumer;
}    
    private async Task ProcessMessageAsync(InputMessage message, CancellationToken ct)
    {
        _logger.LogInformation("⚙️ Processing message {Id} with priority {Priority}...", 
            message.Id, message.Priority);
        
        // 🔥 ТРИГГЕР ДЛЯ ТЕСТА DLQ:
        // Если приоритет 99 — имитируем критическую ошибку, которую нельзя обработать
        if (message.Priority == 99)
        {
            _logger.LogError("💥 Critical error: Priority 99 is forbidden for message {Id}", message.Id);
            throw new InvalidOperationException($"Forbidden priority value: 99");
        }
        
        // 1. Имитация бизнес-логики (трансформация текста)
        var processedText = message.Text.ToUpperInvariant();
        
        // 2. Валидация (неперехватываемая бизнес-ошибка — сразу в DLQ)
        if (message.Priority < 1 || message.Priority > 10)
        {
            _logger.LogWarning("⚠️ Message {Id} has invalid priority {Priority}, marking as skipped", 
                message.Id, message.Priority);
        
            var skippedResult = new OutputMessage(message.Id, message.Text, DateTime.UtcNow, "Skipped");
            await _producer.PublishAsync(skippedResult, routingKey: "output", cancellationToken: ct);
            return;
        }
        
        // 3. Создаём результат обработки
        var result = new OutputMessage(
            InputMessageId: message.Id,
            ProcessedText: processedText,
            ProcessedAt: DateTime.UtcNow,
            Status: "Success");
        
        // 4. Публикуем в output-queue
        await _producer.PublishAsync(result, routingKey: "output", cancellationToken: ct);
        
        _logger.LogInformation("✅ Message {Id} processed and published to output-queue", message.Id);
    }
}
