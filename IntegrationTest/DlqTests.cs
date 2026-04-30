// Тесты механизма Dead Letter Queue (DLQ).
// Проверяют, что сообщения, отклонённые с requeue: false, корректно перенаправляются в очередь мёртвых писем.
// Каждый тест использует уникальные имена очередей для изоляции.

using Application.Interfaces;
using Domain.Configuration;
using Domain.Events;
using Domain.Interfaces;
using Infrastructure.Messaging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using System.Text;
using Xunit;

namespace IntegrationTest;

public class DlqTests : RabbitMqTestBase
{
    [Fact]
    public async Task NackWithRequeueFalse_ShouldMoveMessageToDlq()
    {
        // ARRANGE: Уникальные имена для полной изоляции теста
        var uniqueId = Guid.NewGuid().ToString("N")[..8];
        var settings = new RabbitMqSettings
        {
            HostName = Host, Port = Port, UserName = User, Password = Pass,
            ExchangeName = $"dlq-test-exchange-{uniqueId}", ExchangeType = "direct",
            InputQueueName = $"dlq-input-{uniqueId}", OutputQueueName = $"unused-{uniqueId}", DlqQueueName = $"dlq-queue-{uniqueId}"
        };

        using var setupChannel = Connection!.CreateModel();
        
        // 1. Декларируем DLX-инфраструктуру (обменник + очередь для мёртвых писем)
        var dlxName = $"{settings.ExchangeName}.dlx";
        setupChannel.ExchangeDeclare(dlxName, ExchangeType.Direct, durable: true);
        setupChannel.QueueDeclare(settings.DlqQueueName, durable: true, exclusive: false, autoDelete: false);
        setupChannel.QueueBind(settings.DlqQueueName, dlxName, "dlq");

        // 2. 🔹 КРИТИЧНО: Декларируем входную очередь С аргументами DLX
        // Без этих аргументов RabbitMQ не будет знать, куда перенаправлять отклонённые сообщения
        var args = new Dictionary<string, object>
        {
            { "x-dead-letter-exchange", dlxName },
            { "x-dead-letter-routing-key", "dlq" }
        };
        setupChannel.ExchangeDeclare(settings.ExchangeName, settings.ExchangeType, durable: true);
        setupChannel.QueueDeclare(settings.InputQueueName, durable: true, exclusive: false, autoDelete: false, arguments: args);
        setupChannel.QueueBind(settings.InputQueueName, settings.ExchangeName, "input");

        var serializer = new JsonMessageSerializer();
        // 🔹 Передаем 5-й аргумент: заглушку для метрик
        var producer = new RabbitMqProducer(Connection!, settings, NullLogger<InputQueueConsumer>.Instance, serializer, new NoOpMetricsCollector());

        // ACT
        
        // 1. Публикуем тестовое сообщение во входную очередь
        var msg = new InputMessage(Guid.NewGuid(), "Will go to DLQ", 1, DateTime.UtcNow);
        await producer.PublishAsync(msg, routingKey: "input", exchange: settings.ExchangeName);

        // 2. Забираем сообщение и явно отклоняем его с requeue: false
        using var channel = Connection.CreateModel();
        var received = channel.BasicGet(settings.InputQueueName, autoAck: false);
        Assert.NotNull(received);
        
        // Это ключевое действие: nack с requeue: false триггерит перенаправление в DLQ
        channel.BasicNack(received.DeliveryTag, multiple: false, requeue: false);

        // 3. Ждём асинхронного перенаправления (брокер делает это быстро, но не мгновенно)
        await Task.Delay(1000);

        // ASSERT: Проверяем, что сообщение появилось в очереди мёртвых писем
        var dlqMsg = channel.BasicGet(settings.DlqQueueName, autoAck: true);
        Assert.NotNull(dlqMsg);
        
        var payload = Encoding.UTF8.GetString(dlqMsg!.Body.ToArray());
        Assert.Contains("Will go to DLQ", payload);
        
        // Проверяем системный заголовок x-death — маркер автоматической маршрутизации в DLQ
        Assert.NotNull(dlqMsg.BasicProperties.Headers);
        Assert.True(dlqMsg.BasicProperties.Headers.ContainsKey("x-death"));
    }

    // 🔹 Внутренняя заглушка для сбора метрик (копия из ProducerTests для изоляции)
    private sealed class NoOpMetricsCollector : IMetricsCollector
    {
        public void RecordMessagePublished(string queueName, string messageType) { }
        public void RecordMessageProcessed(string queueName, string messageType, double processingTimeMs) { }
        public void RecordMessageError(string queueName, string messageType, string errorType) { }
        public void RecordQueueDepth(string queueName, int depth) { }
        public string ExportToPrometheusFormat() => string.Empty;
    }
}
