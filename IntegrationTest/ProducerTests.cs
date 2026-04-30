// Тесты для RabbitMqProducer.
// Проверяют: публикацию сообщений в брокер и корректность сериализации/десериализации.
// Включают заглушку для IMetricsCollector, так как в тестах метрики не нужны.

using Application.Interfaces;
using Domain.Configuration;
using Domain.Events;
using Infrastructure.Messaging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using System.Text;
using Xunit;

namespace IntegrationTest;

public class ProducerTests : RabbitMqTestBase
{
    [Fact]
    public async Task PublishAsync_ShouldSendMessageToExchange()
    {
        // ARRANGE: Уникальные имена очередей для изоляции теста
        var uniqueId = Guid.NewGuid().ToString("N")[..8];
        var settings = new RabbitMqSettings
        {
            HostName = Host, Port = Port, UserName = User, Password = Pass,
            ExchangeName = $"test-exchange-{uniqueId}", ExchangeType = "direct",
            InputQueueName = $"test-input-{uniqueId}", OutputQueueName = $"test-output-{uniqueId}", DlqQueueName = $"test-dlq-{uniqueId}"
        };
        
        var serializer = new JsonMessageSerializer();
        // 🔹 Передаем 5-й аргумент: заглушку для метрик
        var producer = new RabbitMqProducer(Connection!, settings, NullLogger<InputQueueConsumer>.Instance, serializer, new NoOpMetricsCollector());
        
        using var channel = Connection!.CreateModel();
        channel.ExchangeDeclare(settings.ExchangeName, settings.ExchangeType, durable: true);
        channel.QueueDeclare(settings.InputQueueName, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(settings.InputQueueName, settings.ExchangeName, "test-key");
        
        // ACT: Публикуем тестовое сообщение
        var message = new InputMessage(Guid.NewGuid(), "Test payload", 3, DateTime.UtcNow);
        await producer.PublishAsync(message, routingKey: "test-key", exchange: settings.ExchangeName);
        
        // ASSERT: Проверяем, что сообщение реально появилось в очереди
        var received = channel.BasicGet(settings.InputQueueName, autoAck: true);
        Assert.NotNull(received);
        
        var payload = Encoding.UTF8.GetString(received.Body.ToArray());
        Assert.Contains("Test payload", payload);
        Assert.Equal("application/json", received.BasicProperties.ContentType);
    }

    [Fact]
    public void SerializeAndDeserialize_ShouldPreserveMessageContent()
    {
        // ARRANGE: Создаём исходное сообщение с известными данными
        var serializer = new JsonMessageSerializer();
        var original = new InputMessage(
            Id: Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Text: "Roundtrip test",
            Priority: 7,
            Timestamp: new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc)
        );

        // ACT: Сериализуем и десериализуем
        var bytes = serializer.Serialize(original);
        var restored = serializer.Deserialize<InputMessage>(bytes);

        // ASSERT: Проверяем, что все поля сохранились без изменений
        Assert.Equal(original.Id, restored.Id);
        Assert.Equal(original.Text, restored.Text);
        Assert.Equal(original.Priority, restored.Priority);
        Assert.Equal(original.Timestamp, restored.Timestamp);
    }

    // 🔹 Внутренняя заглушка для сбора метрик (чтобы не тянуть Moq)
    private sealed class NoOpMetricsCollector : IMetricsCollector
    {
        public void RecordMessagePublished(string queueName, string messageType) { }
        public void RecordMessageProcessed(string queueName, string messageType, double processingTimeMs) { }
        public void RecordMessageError(string queueName, string messageType, string errorType) { }
        public void RecordQueueDepth(string queueName, int depth) { }
        public string ExportToPrometheusFormat() => string.Empty;
    }
}
